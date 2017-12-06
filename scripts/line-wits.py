from __future__ import print_function
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, struct, size, sum

def lineRecord(r):
    res = []
    off1 = r.b1
    off2 = r.b2
    for line in r.pairs:
        res.append(Row(id=r.id2, begin=off2, text=line['_2'],
                       sid=r.id1, sbegin=off1, stext=line['_1']))
        off1 = off1 + len(line['_1'])
        off2 = off2 + len(line['_2'])
    return res

def corpusLines(r):
    res = []
    off = 0
    for line in r.text.splitlines(True):
        res.append(Row(id=r.id, begin=off, text=line))
        off = off + len(line)
    return res

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: line-wits.py <input> <corpus> <output>", file=sys.stderr)
        exit(-1)

    spark = SparkSession.builder.appName('Line Witnesses').getOrCreate()

    spark.read.load(sys.argv[2])\
        .rdd.flatMap(corpusLines)\
        .toDF()\
        .join(spark.read.load(sys.argv[1]).rdd.flatMap(lineRecord).toDF().drop('text'),
              ['id', 'begin'], 'left_outer')\
        .write.save(sys.argv[3])

    spark.stop()
    
