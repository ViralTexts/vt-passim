import json, os, sys
from math import log
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
    if len(sys.argv) != 3:
        print("Usage: vote-collate.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Voting Collate').getOrCreate()

    majority_collate = udf(lambda text, wits: majorityCollate(text, wits))

    oracle_collate = udf(lambda text, srcAlg, dstAlg, wits: oracleCollate(text, srcAlg, dstAlg, wits))

    raw = spark.read.json(sys.argv[1])

    raw.withColumn('maj', majority_collate('dstText', 'wits')
        ).withColumn('oracle', oracle_collate('dstText', 'srcAlg', 'dstAlg', 'wits')
        ).withColumn('srcClean', lower(btrim('srcText', lit(' \n')))
        ).withColumn('dstClean', lower(btrim('dstText', lit(' \n')))
        ).withColumn('majClean', lower(btrim('maj', lit(' \n')))
        ).withColumn('oracleClean', lower(btrim('oracle', lit(' \n')))
        ).withColumn('dstCER', f.levenshtein('srcClean', 'dstClean')/
                     f.greatest(length('srcClean'), length('dstClean'))
        ).withColumn('majCER', f.levenshtein('srcClean', 'majClean')/
                     f.greatest(length('srcClean'), length('majClean'))
        ).withColumn('oraCER', f.levenshtein('srcClean', 'oracleClean')/
                     f.greatest(length('srcClean'), length('oracleClean'))
        ).drop('srcClean', 'dstClean', 'majClean', 'oracleClean'
        ).write.json(sys.argv[2], mode='overwrite')

    spark.stop()
