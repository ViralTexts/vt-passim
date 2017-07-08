from __future__ import print_function
from re import sub
import sys
from os.path import basename, splitext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode

def lineRecord(r):
    res = []
    off = 0
    for line in r['pairs']:
        key = '%s_%d' % (r['id2'], r['b2'] + off)
        res.append(Row(key=key, id1=r['id1'], id2=r['id2'], s1=line['_1'], s2=line['_2']))
        off = off + len(line['_2'])
    return res

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: line-coverage.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Line Coverage').getOrCreate()
    spark.read.json(sys.argv[1])\
        .rdd.flatMap(lineRecord)\
        .toDF()\
        .write.save(sys.argv[2])
    spark.stop()
    
