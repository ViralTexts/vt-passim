import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, collect_set, lit, map_from_entries,
                                   size, sort_array, struct, udf, when, xxhash64, year)
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

    if 'daylag' not in clusters.columns:
        clusters = clusters.withColumn('daylag', lit(0))

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
                                 when(col('period').isNull() |
                                      (col('period') > when(col('daylag') > config.self_lag, col('daylag')).otherwise(config.self_lag)),
                                      lit(0)).otherwise(lit(1))
                    ).filter((col('lag') > 0) | (col('self') == 1)
                    ).withColumn('begin', f.explode(line_starts('text', 'begin'))
                    ).groupBy('uid', 'begin'
                    ).agg(f.max('lag').alias('age'), f.max('self').alias('self')
                    ).groupBy('uid', 'self'
                    ).agg(f.sum('age').alias('tage'),
                          f.sum((col('age') <= 7).cast('int')).alias('a7'),
                          f.sum(((col('age') <= 31) & (col('age') > 7)).cast('int')).alias('a31'),
                          f.sum(((col('age') > 31) & (col('age') <= 366)).cast('int')).alias('a366'),
                          f.sum(f.log1p('age')).alias('lage'),
                          f.count('age').alias('rlines')
                    ).groupBy('uid'
                    ).agg(map_from_entries(collect_set(struct('self',
                                                              struct('tage', 'a7', 'a31', 'a366', 'lage', 'rlines')
                                                              ))).alias('info')
                    ).select('uid', col('info')[0]['tage'].alias('other_tage'),
                             col('info')[1]['tage'].alias('self_tage'),
                             col('info')[0]['a7'].alias('other_7'),
                             col('info')[1]['a7'].alias('self_7'),
                             col('info')[0]['a31'].alias('other_31'),
                             col('info')[1]['a31'].alias('self_31'),
                             col('info')[0]['a366'].alias('other_366'),
                             col('info')[1]['a366'].alias('self_366'),
                             col('info')[0]['lage'].alias('other_lage'),
                             col('info')[1]['lage'].alias('self_lage'),
                             col('info')[0]['rlines'].alias('other_rlines'),
                             col('info')[1]['rlines'].alias('self_rlines')
                    ).na.fill(0)
    
    corpus = spark.read.load(config.corpusPath, mergeSchema=True
                ).drop('pp'
                ).withColumn('uid', xxhash64('id')
                ).withColumn('pnum', coalesce((col('pos') + 1),
                                     f.array_max(col('pages')['seq']),
                                     col('seq')) + f.when(col('corpus') == 'ia', 1).otherwise(0)
                ).withColumn('lines', size(f.split('text', '\n')))

    issues = corpus.groupBy('issue').agg(f.max('pnum').alias('pp'))

    corpus.join(issues, 'issue'
         ).join(lineage, ['uid'], 'left_outer'
         ).na.fill(0
         ).groupBy('series', 'corpus', f.year('date').alias('year'), 'pp', 'pnum',
         ).agg(f.countDistinct('issue').alias('issues'),
               f.sum('lines').alias('lines'),
               f.sum('other_tage').alias('other_tage'),
               f.sum('other_7').alias('other_7'),
               f.sum('other_31').alias('other_31'),
               f.sum('other_366').alias('other_366'),
               f.sum('other_lage').alias('other_lage'),
               f.sum('other_rlines').alias('other_rlines'),
               f.sum('self_tage').alias('self_tage'),
               f.sum('self_7').alias('self_7'),
               f.sum('self_31').alias('self_31'),
               f.sum('self_366').alias('self_366'),
               f.sum('self_lage').alias('self_lage'),
               f.sum('self_rlines').alias('self_rlines')
         ).sort('series', 'corpus', 'year', 'pp', 'pnum'
         ).coalesce(1
         ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
