import json, os, sys
from math import log
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, to_json, from_json, struct, length
from pyspark.sql.types import ArrayType, StringType

def majorityCollate(text, wits):
    cols = list()
    for c in text:
        cols.append({'': 1.01})
        cols.append({c: 1.01})
    # cols.append({'': 1.01})  # alg2 ends in \n by construction

    for wit in wits:
        idx = 0
        insert = ''
        for w, t in zip(wit.alg1.replace('\xad\n', '--').replace('\n', ' '), wit.alg2):
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
    return res.strip() + '\n'

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
