import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, collect_set, lit, map_from_entries,
                                   size, sort_array, struct, when, udf, when, xxhash64, year)
import pyspark.sql.functions as f

def groupPeriod(lags, min_lag):
    if min_lag < 0:
        min_lag = 0
    res = []
    last = {}
    for w in lags:
        prev = last.get(w.group)
        last[w.group] = w.lag
        if (prev != None) and (w.lag - prev >= min_lag):
            res.append((w.lag - prev, w.issue, w.group))
    return res

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
    parser.add_argument('-s', '--self-lag', type=int, default=31,
                        help='Maximum lag for self reprinting')
    parser.add_argument('clusterPath', metavar='<path>', help='cluster path')
    parser.add_argument('corpusPath', metavar='<path>', help='corpus path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Compute ages of lines using cluster data').getOrCreate()

    clusters = spark.read.load(config.clusterPath)

    group_period = udf(lambda lags: groupPeriod(lags, 0),
                     'array<struct<period: int, issue: string, group: string>>')
    line_starts = udf(lambda text, begin: lineStarts(text, begin), 'array<int>')

    clags = clusters.groupBy('cluster'
                ).agg(f.min('date').alias('cstart')
                ).join(clusters, 'cluster'
                ).withColumn('lag', f.datediff('date', 'cstart'))

    periods = clags.groupBy('cluster', 'cstart'
                ).agg(group_period(sort_array(collect_set(struct('lag', 'group', 'issue')))
                                   ).alias('periods')
                ).select('cluster', 'cstart', f.explode('periods').alias('p')
                ).select('cluster', 'cstart', col('p.*')
                ).select('cluster', 'issue', 'period')

    lineage = clags.join(periods, ['cluster', 'issue'], 'left_outer'
                    ).withColumn('self',
                                 when(col('period').isNull() | (col('period') > config.self_lag),
                                      lit(0)).otherwise(lit(1))
                    ).withColumn('begin', f.explode(line_starts('text', 'begin'))
                    ).groupBy('uid', 'begin'
                    ).agg(f.max('lag').alias('age'), f.max('self').alias('self')
                    ).groupBy('uid', 'self'
                    ).agg(f.sum('age').alias('tage'), f.count('age').alias('rlines')
                    ).groupBy('uid'
                    ).agg(map_from_entries(collect_set(struct('self', 'tage'))).alias('tage'),
                          map_from_entries(collect_set(struct('self', 'rlines'))).alias('rlines')
                    ).select('uid', col('tage')[0].alias('other_tage'),
                             col('tage')[1].alias('self_tage'),
                             col('rlines')[0].alias('other_rlines'),
                             col('rlines')[1].alias('self_rlines')
                    ).na.fill(0)
    
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
         ).groupBy('series', f.year('date').alias('year'), 'pp', 'pnum',
         ).agg(f.countDistinct('issue').alias('issues'),
               f.sum('lines').alias('lines'),
               f.sum('other_tage').alias('other_tage'),
               f.sum('other_rlines').alias('other_rlines'),
               f.sum('self_tage').alias('self_tage'),
               f.sum('self_rlines').alias('self_rlines')
         ).sort('series', 'year', 'pp', 'pnum'
         ).coalesce(1
         ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
