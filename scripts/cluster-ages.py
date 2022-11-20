import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, size, struct, when, udf, xxhash64, year)
import pyspark.sql.functions as f

def lineStarts(text, begin):
    res = []
    off = begin
    for line in text.splitlines(keepends=True):
        if off > begin:
            res.append(off)
        off += len(line)
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute ages of lines using cluster data',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clusterPath', metavar='<path>', help='cluster path')
    parser.add_argument('corpusPath', metavar='<path>', help='corpus path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Compute ages of lines using cluster data').getOrCreate()

    clusters = spark.read.load(config.clusterPath)

    line_starts = udf(lambda text, begin: lineStarts(text, begin), 'array<int>')

    cstarts = clusters.groupBy('cluster').agg(f.min('date').alias('cstart'))

    lineage = clusters.join(cstarts, 'cluster'
                    ).withColumn('lag', f.datediff('date', 'cstart')
                    ).withColumn('begin', f.explode(line_starts('text', 'begin'))
                    ).groupBy('uid', 'begin'
                    ).agg(f.max('lag').alias('age')
                    ).groupBy('uid'
                    ).agg(f.sum('age').alias('tage'), f.count('age').alias('rlines'))
    
    corpus = spark.read.load(config.corpusPath, mergeSchema=True
                ).drop('pp'
                ).withColumn('uid', xxhash64('id')
                ).withColumn('pnum', coalesce((col('pos') + 1),
                                     f.array_max(col('pages')['seq']),
                                     col('seq')) + f.when(col('corpus') == 'ia', 1).otherwise(0)
                ).withColumn('lines', size(f.split('text', '\n')))

    issues = corpus.groupBy('issue'
                  ).agg(coalesce(f.max('pnum'), f.countDistinct('pageno')).alias('pp'))

    corpus.join(issues, 'issue'
         ).join(lineage, ['uid'], 'left_outer'
         ).na.fill(0
         ).groupBy('series', 'date', 'issue', 'pp', 'pnum', 'lines'
         ).agg(f.sum('tage').alias('tage'), f.sum('rlines').alias('rlines')
         ).groupBy('series', f.year('date').alias('year'), 'pp', 'pnum'
         ).agg(f.countDistinct('issue').alias('issues'),
               f.sum('tage').alias('tage'),
               f.sum('rlines').alias('rlines'),
               f.sum('lines').alias('lines')
         ).sort('series', 'year', 'pp', 'pnum'
         ).coalesce(1
         ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
