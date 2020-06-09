import json, os, sys
from math import log
import openfst_python as fst
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, to_json, from_json, struct, length
from pyspark.sql.types import ArrayType, StringType

def charset(strings):
    chars = set()
    for s in strings:
        for c in s:
            chars.add(c)
    return chars

def symbesc(s):
    return s.replace(' ', '<space>').replace('\n', '<space>').replace('@', '<epsilon>')

def symbolTable(charset):
    stab = fst.SymbolTable()
    stab.add_symbol('<epsilon>', 0)
    idx = 0
    for c in charset:
        idx += 1
        stab.add_symbol(symbesc(c), idx)
    return stab

def votingMachine(text, wits):
    stable = symbolTable(charset([text] + [w['alg1'] for w in wits]))
    compiler = fst.Compiler(acceptor=True, isymbols=stable, keep_isymbols=True, arc_type='log')
    #compiler = sys.stderr
    tweight = -log(1.01)
    state = 0
    for c in text:
        cc = symbesc(c)
        print('%d %d <epsilon>' % (state, state+1), file=compiler)
        state += 1
        print('%d %d %s %f' % (state, state+1, cc, tweight), file=compiler)
        state += 1
    print('%d %d <epsilon>' % (state, state+1), file=compiler)
    state += 1
    final = state
    print('%d' % (final), file=compiler)
    for wit in wits:
        pos = 0
        w = wit['alg1']
        t = wit['alg2']
        for i in range(0, len(t)):
            if t[i] != '-':
                c = w[i]
                pos += 1
                if i > 0 and t[i-1] == '-':
                    j = i - 1
                    istates = list()
                    while j > 0:
                        if t[j-1] != '-':
                            break
                        j -= 1
                        state += 1
                        istates.append(state)
                    for s1, s2, sym in zip([(pos-1)*2] + istates, istates + [(pos*2)-1], w[j:i]):
                        print('%d %d %s' % (s1, s2, symbesc(sym)), file=compiler)
                out = symbesc(c).replace('-', '<epsilon>')
                print('%d %d %s' % ((pos*2)-1, pos*2, out), file=compiler)
    a = compiler.compile()
    return a

def pathStr(a):
    res = ''
    stab = a.input_symbols()
    for state in a.states():
        for arc in a.arcs(state):
            c = stab.find(arc.ilabel)
            if c == '<space>': c = ' '
            if c == '<nl>': c = '\n'
            res += c
    return res

def majorityCollate(text, wits):
    # return text + str([{'alg1': w.alg1, 'alg2': w.alg2} for w in wits])
    acc = votingMachine(text, [{'alg1': w.alg1.replace('\xad\n', '@@'), 'alg2': w.alg2} for w in wits[0:20]])
    acc.rmepsilon()
    z = fst.shortestpath(fst.arcmap(fst.determinize(acc).minimize(), map_type='to_standard'))
    res = pathStr(z.topsort()).strip() + '\n'
    del acc
    del z
    return res

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: vote-collate.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Voting Collate').getOrCreate()

    majority_collate = udf(lambda text, wits: majorityCollate(text, wits))

    raw = spark.read.json(sys.argv[1])

    raw.select(col('id'), explode('lines').alias('line')) \
       .select(col('id'), col('line.begin').alias('begin'), col('line.text').alias('text'),
               col('line.wits').alias('wits')) \
       .filter((size('wits') >= 2) & (length(col('text')) > 1)) \
       .withColumn('maj', majority_collate('text', 'wits')) \
       .write.json(sys.argv[2])

    spark.stop()
