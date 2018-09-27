from __future__ import print_function
from re import sub
import sys
from os.path import basename, splitext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

def parseCTS(f):
    res = dict()
    text = ''
    locs = []
    id = (splitext(basename(f[0])))[0]
    for line in f[1].split('\n'):
        if line != '':
            (loc, raw) = line.split('\t', 2)
            parts = loc.split(':')
            if len(parts) >= 4: id = ':'.join(parts[0:4])
            start = len(text)
            text += sub('\s+', ' ', raw)
            text += '\n'
            locs.append(Row(start=start, length=(len(text) - start),
                            loc=sub('([^\.]+\.[^\.]+)\.[^\.]+(:[^:]+)$', '\\1\\2', loc)))

    return Row(id=id, series=sub('([^\.]+\.[^\.]+)\.[^\.]+$', '\\1', id), text=text, locs=locs)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: cts-load.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Load CTS TSV').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.sparkContext.wholeTextFiles(sys.argv[1]) \
         .filter(lambda f: f[0].endswith('.cts')) \
         .map(lambda f: parseCTS(f)) \
         .toDF() \
         .withColumn('locs',
                     col('locs').cast('array<struct<length: int, loc: string, start: int>>')) \
         .write.save(sys.argv[2])
    spark.stop()
    
