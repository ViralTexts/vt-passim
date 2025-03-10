import argparse, json, os, sys
from math import inf, log
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, lit, udf, struct, btrim, lower, length
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
        idx = 0
        insert = ''
        for w, t in zip(wit.srcAlg.replace('\xad\n', '--').replace('\n', ' '), wit.dstAlg):
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

def ngramFeatures(s, n):
    s = ('#' * (n-1)) + s
    return [s[(i-n):i] for i in range(n, len(s))]

def lmVote(text, wits, n, pcount, bccounts):
    if wits == None or len(wits) < 1:
        return text
    counts = bccounts.value

    V = 256

    cand = [text.strip()] + [w.srcAlg.replace('-', '').strip() for w in wits]

    maxScore = -inf
    argMaxScore = text.strip() + '\n'
    for s in cand:
        score = 0
        for gram in ngramFeatures(s.lower(), n):
            history = gram[0:(n-1)]
            score += log((counts.get(gram, 0) + pcount) / (counts.get(history, 0) + V))
        score /= len(s)
        if score > maxScore:
            maxScore = score
            argMaxScore = s + '\n'

    return argMaxScore
        

## This method suffers from spurious ambiguity due to mismatching alignments
def oracleCollate(text, srcAlg, dstAlg, wits):
    if wits == None or len(wits) == 0:
        return text
    
    cols = list()
    for c in text:
        cols.append({'': 1.01})
        cols.append({c: 1.01})

    for wit in wits:
        idx = 0
        insert = ''
        for w, t in zip(wit.srcAlg.replace('\xad\n', '--').replace('\n', ' '), wit.dstAlg):
            w = w.replace('-', '')
            if t == '-':
                insert += w
            else:
                cols[idx*2][insert] = cols[idx*2].get(insert, 0) + 1
                insert = ''
                cols[idx*2 + 1][w] = cols[idx*2 + 1].get(w, 0) + 1
                idx += 1

    res = ''
    idx = 0
    for w, t in zip(srcAlg.replace('\xad\n', '--').replace('\n', ' '), dstAlg):
        w = w.replace('-', '')
        if t == '-':
            insert += w
        else:
            if insert in cols[idx*2]:
                res += insert
            insert = ''
            if w in cols[idx*2 + 1]:
                res += w
            else:
                res += t
            idx += 1
        
    return res.strip() + ('\xad\n' if text.endswith('\xad\n') else '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Vote on aligned lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-n', '--n', type=int, default=4, help='n-gram order', metavar='N')
    parser.add_argument('-m', '--min-count', type=int, default=5,
                        help='min n-gram count', metavar='N')
    parser.add_argument('-p', '--pcount', type=float, default=1.0,
                        help='Laplace smoothing', metavar='p')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    majority_collate = udf(lambda text, wits: majorityCollate(text, wits))

    oracle_collate = udf(lambda text, srcAlg, dstAlg, wits: oracleCollate(text, srcAlg, dstAlg, wits))

    ngrams = udf(lambda s: ngramFeatures(s, config.n), 'array<string>')
    
    raw = spark.read.json(config.inputPath)

    grams = raw.select(explode(f.array_append(f.coalesce(col('wits.srcAlg'), f.array()),
                                              col('dstAlg'))).alias('text')
              ).select(f.lower(btrim(f.translate('text', '-', ''), lit(' \n'))).alias('text')
              ).select(explode(ngrams('text')).alias('gram')
              ).groupBy('gram'
              ).count(
              ).filter(col('count') >= config.min_count)

    counts = {}
    for g in grams.collect():
        (gram, count) = g
        counts[gram] = count
        history = gram[0:(config.n-1)]
        counts[history] = counts.get(history, 0) + count

    bccounts = spark.sparkContext.broadcast(counts)

    lm_vote = udf(lambda text, wits: lmVote(text, wits, config.n, config.pcount, bccounts))
    
    raw.withColumn('maj', majority_collate('dstText', 'wits')
        ).withColumn('lmvote', lm_vote('dstText', 'wits')
        ).withColumn('oracle', oracle_collate('dstText', 'srcAlg', 'dstAlg', 'wits')
        ).withColumn('srcClean', lower(btrim('srcText', lit(' \n')))
        ).withColumn('dstClean', lower(btrim('dstText', lit(' \n')))
        ).withColumn('majClean', lower(btrim('maj', lit(' \n')))
        ).withColumn('lmClean', lower(btrim('lmvote', lit(' \n')))
        ).withColumn('oracleClean', lower(btrim('oracle', lit(' \n')))
        ).withColumn('dstCER', f.levenshtein('srcClean', 'dstClean')/
                     f.greatest(length('srcClean'), length('dstClean'))
        ).withColumn('lmCER', f.levenshtein('srcClean', 'lmClean')/
                     f.greatest(length('srcClean'), length('lmClean'))
        ).withColumn('majCER', f.levenshtein('srcClean', 'majClean')/
                     f.greatest(length('srcClean'), length('majClean'))
        ).withColumn('oraCER', f.levenshtein('srcClean', 'oracleClean')/
                     f.greatest(length('srcClean'), length('oracleClean'))
        ).drop('srcClean', 'dstClean', 'majClean', 'lmClean', 'oracleClean'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
