import argparse, json, os, sys
from math import inf, log
import pywrapfst as fst
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (col, size, lit, udf, struct, greatest,
                                   translate, transform, btrim, lower, length)
import pyspark.sql.functions as f

def majorityCollate(text, wits):
    if wits == None or len(wits) <= 1:
        return text
    
    cols = list()
    for c in text:
        cols.append({'': 1.01})
        cols.append({c: 1.01})
    # cols.append({'': 1.01})  # alg2 ends in \n by construction

    for wit in wits:
        (matchRate, id, begin, srcText, srcAlg, dstAlg) = wit
        idx = 0
        insert = ''
        for w, t in zip(srcAlg.replace('\xad\n', '--').replace('\n', ' '), dstAlg):
            w = w.replace('-', '')
            if t == '-':
                insert += w
            else:
                cols[idx*2][insert] = cols[idx*2].get(insert, 0) + 1
                insert = ''
                cols[idx*2 + 1][w] = cols[idx*2 + 1].get(w, 0) + 1
                idx += 1

    res = ''
    for col in cols:
        res += max(col.items(), key=lambda i: i[1])[0]
    return res.strip() + ('\xad\n' if text.endswith('\xad\n') else '\n')

def wurstCollate(text, wits, brlm, bonus=0.01):
    if wits == None or len(wits) == 0:
        return text
    
    lm = brlm.value
    syms = lm.input_symbols()

    cols = list()
    for c in text:
        cols.append({'': 1 + bonus})
        cols.append({c: 1 + bonus})

    for wit in wits:
        (matchRate, id, begin, srcText, srcAlg, dstAlg) = wit
        idx = 0
        insert = ''
        for w, t in zip(srcAlg.replace('\xad\n', '--').replace('\n', ' '), dstAlg):
            w = w.replace('-', '')
            if t == '-':
                insert += w
            else:
                cols[idx*2][insert] = cols[idx*2].get(insert, 0.0) + 1
                insert = ''
                cols[idx*2 + 1][w] = cols[idx*2 + 1].get(w, 0.0) + 1
                idx += 1

    compiler = fst.Compiler()
    den = 1 + len(wits) + bonus
    freeState = len(cols)
    for i in range(0, len(cols)-1):
        for k, v in cols[i].items():
            if len(k) <= 1:
                c = ord(k) if k != '' else 0
                print(f'{i} {i+1} {c} {c} {-log(v/den)}', file=compiler)
            else:
                start = i
                end = freeState
                w = -log(v/den)
                for j in range(len(k) - 1):
                    c = ord(k[j])
                    print(f'{start} {end} {c} {c} {w}', file=compiler)
                    start = end
                    freeState += 1
                    end = freeState
                    w = 0
                c = ord(k[-1])
                print(f'{start} {i+1} {c} {c} 0', file=compiler)

    print(f'{len(cols)-1}', file=compiler)

    m = compiler.compile()
    m.rmepsilon()
    top = fst.shortestpath(fst.intersect(m, lm))
    top.rmepsilon()
    top.topsort()
    return ''.join([c if c != '<space>' else ' ' for c
                    in [(c.split())[2] for c
                        in top.print(isymbols=syms, acceptor=True).split('\n')
                        if len(c.split()) > 2]]) + ('\xad\n' if text.endswith('\xad\n') else '\n')

def levCollate(text, wits, brlm, bonus=0.01):
    if wits == None or len(wits) == 0:
        return text
    
    lm = brlm.value
    syms = lm.input_symbols()

    cols = list()
    obs = list()
    for c in text:
        cols.append({'': 1 + bonus})
        cols.append({c: 1 + bonus})
        obs.append('')
        obs.append(c)

    for wit in wits:
        (matchRate, id, begin, srcText, srcAlg, dstAlg) = wit
        idx = 0
        insert = ''
        ## Only sub \xad\n in the middle not end of srcAlg
        for w, t in zip(srcAlg.replace('\xad\n', '--').replace('\n', ' '), dstAlg):
            w = w.replace('-', '')
            if t == '-':
                insert += w
            else:
                cols[idx*2][insert] = cols[idx*2].get(insert, 0.0) + 1
                insert = ''
                cols[idx*2 + 1][w] = cols[idx*2 + 1].get(w, 0.0) + 1
                idx += 1

    compiler = fst.Compiler()
    den = 1 + len(wits) + bonus
    freeState = len(cols)
    pcopy = 0.1
    nlcopy = -log(pcopy)
    nledit = -log((1 - pcopy) / (2 * 256))
    for i in range(0, len(cols)-1):
        for k, v in cols[i].items():
            if len(k) <= 1:
                c = ord(k) if k != '' else 0
                if k == obs[i]:
                    if k == '':
                        w = 0
                    else:
                        w = nlcopy
                else:
                    w = nledit
                print(f'{i} {i+1} {c} {c} {w - log(v/den)}', file=compiler)
                #print(f'{i} {i+1} {c} {c} {w}', file=compiler)
                #print(f'{i} {i+1} {c} {c} {-log(v/den)}', file=compiler)
            else:
                start = i
                end = freeState
                w = -log(v/den)
                for j in range(len(k) - 1):
                    c = ord(k[j])
                    #print(f'{start} {end} {c} {c} {nledit}', file=compiler)
                    print(f'{start} {end} {c} {c} {nledit + w}', file=compiler)
                    start = end
                    freeState += 1
                    end = freeState
                    w = 0
                c = ord(k[-1])
                print(f'{start} {i+1} {c} {c} {nledit}', file=compiler)

    print(f'{len(cols)-1}', file=compiler)

    m = compiler.compile()
    m.rmepsilon()
    top = fst.shortestpath(fst.intersect(m, lm))
    top.rmepsilon()
    top.topsort()
    return ''.join([c if c != '<space>' else ' ' for c
                    in [(c.split())[2] for c
                        in top.print(isymbols=syms, acceptor=True).split('\n')
                        if len(c.split()) > 2]]) + ('\xad\n' if text.endswith('\xad\n') else '\n')


def getTransitions(src, dst):
    return [(s, d) for s, d in zip(src, dst)]

def saccept(s):
    compiler = fst.Compiler()
    for i in range(0, len(s)):
        c = ord(s[i])
        print(f'{i} {i+1} {c} {c}', file=compiler)
    print(f'{len(s)}', file=compiler)
    return compiler.compile()

def editCorrect(text, brlm, bredits):
    if bredits == None:
        return text
    lm = brlm.value
    syms = lm.input_symbols()

    edits = bredits.value

    sm = saccept(text.replace('\xad\n', '').strip())
    em = fst.compose(sm, edits)
    em.project('output')
    em.prune(weight=3)
    em.rmepsilon()
    em.arcsort()

    top = fst.shortestpath(fst.intersect(em, lm), nshortest=1)
    top.rmepsilon()
    top.topsort()
    res = ''.join([c if c != '<space>' else ' ' for c
                   in [(c.split())[2] for c
                       in top.print(isymbols=syms, acceptor=True).split('\n')
                       if len(c.split()) > 2]]) + ('\xad\n' if text.endswith('\xad\n') else '\n')
    if len(res.strip()) == 0:
        return text
    else:
        return res

# length of the maximum alignment gap
def max_gap(s):
    res = 0
    cur = 0
    for c in s:
        if c == '-':
            cur += 1
        elif cur > 0:
            if cur > res:
                res = cur
            cur = 0
    if cur > res:
        res = cur
    return res

def fixHyphen(src, dst):
    if len(src) >= 3 and len(dst) >= 3 and dst.endswith('\u2010\n') and dst[-3] != '-' and src[-3] != '-' and src.endswith('--'):
        src = src[:(len(src)-2)] + '\u2010\n'
    return src

def fixCase(src, dst):
    res = list(src)
    i = 0
    while i < (len(res)-1):
        if res[i] != dst[i] and res[i].lower() == dst[i].lower() and res[i+1] == dst[i+1] and (i == 0 or res[i-1] == dst[i-1]):
            res[i] = dst[i]
        i += 1
    return ''.join(res)

def fixLongs(src, dst):
    res = list(src)
    i = 0
    while i < (len(res)-1):
        if res[i] == 's' and (dst[i] == '\u017F' or dst[i] == 'f'):
            res[i] = '\u017F'
        i += 1
    return ''.join(res)

def digitMatch(src, dst):
    "Intersection-over-union of digits"
    union = 0
    inter = 0
    for i in range(len(src)):
        if src[i].isdigit():
            union += 1
            if src[i] == dst[i]:
                inter += 1
        elif dst[i].isdigit():
            union += 1
    if union > 0:
        return inter / union
    else:
        return 1.0

def lead_gap(s):
    res = 0
    for c in s:
        if c.isspace():
            continue
        if c != '-':
            break
        res += 1
    return res

def tail_gap(s):
    res = 0
    seq = list(s)
    seq.reverse()
    for c in seq:
        if c.isspace():
            continue
        if c != '-':
            break
        res += 1
    return res

def goodAlg(config, text, wit):
    llen = len(text.strip())
    srcAlg = wit.alg
    dstAlg = wit.alg2
    algLen = len(dstAlg.replace('-', '').strip())
    matchRate = wit.matches / max(len(text), len(wit.text))
    maxGap = max(max_gap(srcAlg), max_gap(dstAlg))
    leadGap = max(lead_gap(srcAlg), lead_gap(dstAlg))
    tailGap = max(tail_gap(srcAlg), tail_gap(dstAlg))
    if (llen == algLen and matchRate > 0.5 and maxGap < 4
        and leadGap <= 1 and tailGap <= 1):
        return True
    else:
        return False

def fixLines(config, brlm, bredits, lines, pages):
    if pages == 0:
        pages = None
    res = []
    p = 0
    i = 0
    for line in lines:
        llen = len(line.text.strip())
        wits = []
        gold = None
        if llen >= config.min_line and llen <= config.max_line and line.wits != None:
            for w in line.wits:
                if 'ref' in w and w.ref == 1:
                    gold = w.text
                    continue
                algLen = len(w.alg2.replace('-', '').strip())
                dstAlg = w.alg2
                srcAlg = fixHyphen(w.alg, dstAlg)
                if config.fix_case:
                    srcAlg = fixCase(srcAlg, dstAlg)
                if config.fix_longs:
                    srcAlg = fixLongs(srcAlg, dstAlg)
                matchRate = w.matches / max(len(line.text), len(w.text))
                maxGap = max(max_gap(srcAlg), max_gap(dstAlg))
                leadGap = max(lead_gap(srcAlg), lead_gap(dstAlg))
                tailGap = max(tail_gap(srcAlg), tail_gap(dstAlg))
                if (llen == algLen and matchRate > 0.5 and maxGap < 4
                    and leadGap <= 1 and tailGap <= 1):
                    wits.append((matchRate, w.id, w.begin, w.text, srcAlg, dstAlg))
            wits.sort(reverse=True)
            wits = wits[:(config.max_wits)]
        if len(wits) < 1:
            wits = None
        while (pages != None and i < len(pages[p].regions)
               and pages[p].regions[i].start < line.begin):
            i += 1
        end = line.begin + len(line.text)
        x1, y1, x2, y2 = inf, inf, 0, 0
        while (pages != None and i < len(pages[p].regions) and
               (pages[p].regions[i].start + pages[p].regions[i].length) < end):
            coords = pages[p].regions[i].coords
            x1, y1 = min(x1, coords.x), min(y1, coords.y)
            x2, y2 = max(x2, coords.x + coords.w), max(y2, coords.y + coords.h)
            i += 1
        if x1 == inf or y1 == inf:
            x1, y1, w, h = None, None, None, None
        else:
            x1 = int(x1)
            y1 = int(y1)
            w = int(x2-x1)
            h = int(y2-y1)
        maj = majorityCollate(line.text, wits)
        wurst = wurstCollate(line.text, wits, brlm)
        edits = levCollate(line.text, wits, brlm)
        # edits = editCorrect(line.text, brlm, bredits) if (wits == None or len(wits) <= 1) else maj
        res.append((line.begin, line.text, maj, wurst, edits, gold, x1, y1, w, h, wits))
    return res

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collate docwise witnesses',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-m', '--model', help='model file')
    parser.add_argument('--min-line', type=int, default=2,
                         help='Minimum length of line', metavar='N')
    parser.add_argument('--max-line', type=int, default=100,
                         help='Maximum length of line', metavar='N')
    parser.add_argument('--max-wits', type=int, default=20,
                         help='Maximum witnesses per line', metavar='N')
    parser.add_argument('--edits', action='store_true',
                        help='Estimate edit distance.')
    parser.add_argument('--fix-case', action='store_true',
                        help='Match case in destination.')
    parser.add_argument('--fix-longs', action='store_true',
                        help='Infer underlying long s from destination.')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    lm = fst.Fst.read(config.model)
    brlm = spark.sparkContext.broadcast(lm)

    raw = spark.read.load(config.inputPath)
    if 'pages' not in raw.columns:
        raw = raw.withColumn('pages', lit(0))
        
    if config.edits:
        good_alg = udf(lambda text, wit: goodAlg(config, text, wit), 'boolean')
        transitions = udf(lambda src, dst: getTransitions(src, dst),
                          'array<struct<s: string, t: string>>')

        trans = raw.select(f.explode('lines').alias('line')
                  ).select('line.*'
                  ).select('text', f.explode('wits').alias('wit')
                  ).filter(good_alg('text', 'wit')
                  ).select(f.explode(transitions(translate('wit.alg', '\n\t', '  '),
                                                 translate('wit.alg2', '\n\t', '  '))).alias('pair')
                  ).filter(col('pair.s') != '-'
                  ).filter(col('pair.t') != '-'
                  ).groupBy('pair.s', 'pair.t'
                  ).count(
                  ).filter(col('count') >= 5
                  ).groupBy('t'
                  ).agg(f.sum('count').alias('total'),
#                    f.slice(f.sort_array(f.collect_list(struct('count', 's')), asc=False), 1, 10)
                        f.sort_array(f.collect_list(struct('count', 's')), asc=False)
                  ).sort('t')

        compiler = fst.Compiler()
        pccopy = 1000
        for orig in trans.collect():
            (t, total, outs) = orig
            ts = ord(t) if t != '-' else 0
            if t == '\xad':
                ts = ord('-')
            for arc in outs:
                (count, s) = arc
                print(t, s, count, count/total)
                ss = ord(s) if s != '-' else 0
                if s == '\xad':
                    ss = ord('-')
                print(f'0 0 {ts} {ss} {-log(count/total)}', file=compiler)
        print(f'0', file=compiler)

        edits = compiler.compile()
        # edits.write('edits.fst')
        bredits = spark.sparkContext.broadcast(edits)
    else:
        bredits = None


    digit_match = udf(lambda src, dst: digitMatch(src, dst), 'double')

    fix_lines = udf(lambda lines, pages: fixLines(config, brlm, bredits, lines, pages),
                    'array<struct<begin: int, text: string, maj: string, cnlm: string, edits: string, gold: string, x: int, y: int, w: int, h: int, wits: array<struct<matchRate: double, id: string, begin: int, text: string, srcAlg: string, dstAlg: string>>>>')

    raw.withColumn('lines', fix_lines('lines', 'pages')
      ).write.save(config.outputPath, mode='overwrite')
                                        
    spark.stop()
