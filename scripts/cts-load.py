from __future__ import print_function
from re import sub
import sys
from os.path import basename, splitext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

def parseCTS(f):
    res = list()
    text = ''
    locs = []
    curid = ''
    id = ''
    seq = 0
    for line in f[1].split('\n'):
        if line != '':
            (loc, raw) = line.split('\t', 2)
            id = sub('^(.+[^\.:]+)\.[^\.:]*$', '\\1', loc)
            if curid != id:
                if curid != '':
                    res.append(Row(id=curid,
                                   seq=seq,
                                   series=sub('([^\.]+\.[^\.]+)\.[^\.]+(:[^:]+)$', '\\1', curid),
                                   text=text, locs=locs))
                    text = ''
                    locs = []
                    seq += 1
                curid = id
            start = len(text)
            text += sub('\s+', ' ', raw)
            text += '\n'
            locs.append(Row(start=start, length=(len(text) - start),
                            loc=sub('([^\.]+\.[^\.]+)\.[^\.]+(:[^:]+)$', '\\1\\2', loc)))

    if curid != '':
        res.append(Row(id=curid,
                       seq=seq,
                       series=sub('([^\.]+\.[^\.]+)\.[^\.]+(:[^:]+)$', '\\1', curid),
                       text=text, locs=locs))
    return res

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: cts-load.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Load CTS TSV').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.sparkContext.wholeTextFiles(sys.argv[1]) \
         .filter(lambda f: f[0].endswith('.cts')) \
         .flatMap(lambda f: parseCTS(f)) \
         .toDF() \
         .withColumn('locs',
                     col('locs').cast('array<struct<length: int, loc: string, start: int>>')) \
         .withColumn('seq', col('seq').cast('int')) \
         .write.save(sys.argv[2])
    spark.stop()
    
