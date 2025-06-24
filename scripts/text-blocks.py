from __future__ import print_function
from re import sub, split
from os.path import basename, splitext
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

def fileRecords(f):
    res = list()
    book, ext = splitext(basename(f[0]))
    seq = -1
    for block in split('\n\n\n+', f[1]):
        seq += 1
        id = '%s#s%05d' % (book, seq)
        res.append(Row(id=id, book=book, seq=seq,
                       text=block.replace('&gt;', '>')\
                                 .replace('&lt;', '<')))
    return res

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: text-files.py <input> <output>", file=sys.stderr)
        exit(-1)

    groupField = 'book'
    spark = SparkSession.builder.appName('Load Whole Text Files').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')
    spark.sparkContext.wholeTextFiles(sys.argv[1])\
        .flatMap(lambda f: fileRecords(f))\
        .toDF()\
        .write.save(sys.argv[2])
    spark.stop()
