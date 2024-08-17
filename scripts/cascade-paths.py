import argparse, os
from math import log, exp
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, lit, regexp_replace, struct, udf
import pyspark.sql.functions as f

def linkLags(links):
    res = []
    deps = {(r.dst):r for r in links}
    for r in links:
        city2 = r.city
        seen = set([city2])
        lag = r.lag
        lp = log(r.post)
        orig = r.src
        while orig > 0:
            cur = deps[orig]
            res.append((cur.city, city2, r.freq, lag, exp(lp)))
            if cur.city in seen:
                break
            seen.add(cur.city)
            lag += cur.lag
            lp += log(cur.post)
            orig = cur.src
        return res            

## input is sorted by lags.lag
def lagStats(lags):
    count = sum(r.p for r in lags)
    tp = 0
    lag25p = None
    lag50p = None
    lag75p = None
    for r in lags:
        tp += r.p
        quant = tp / count
        if lag25p == None and quant >= 0.25:
            lag25p = r.lag
        if lag50p == None and quant >= 0.5:
            lag50p = r.lag
        if lag75p == None and quant >= 0.75:
            lag75p = r.lag
    return (lag25p, lag50p, lag75p, count)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Lag of posteriors from cascade models',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    link_lags = udf(lambda links: linkLags(links),
                    'array<struct<city:string, city2:string, freq:string, lag:int, p:double>>')

    lag_stats = udf(lambda lags: lagStats(lags),
                    'struct<lag25p: int, lag50p: int, lag75p: int, count: double>')

    spark.read.load(config.inputPath
        ).filter( col('label') == 1
        # ).filter( col('city2').isNotNull()
        ).groupBy('cluster', 'dst', col('freq2').alias('freq'),
                  regexp_replace('city2', '^http://dbpedia.org/resource/', '').alias('city')
        ).agg((f.max(struct('post', 'src', 'label', 'lag'))).alias('src')
        ).select('cluster', 'dst', 'freq', 'city', 'src.*'
        # ).filter( col('src') > 0
        ).groupBy('cluster'
        ).agg(collect_list(struct('dst', 'src', 'freq', 'city', 'lag', 'post')).alias('links')
        ).select(explode(link_lags('links')).alias('lag')).select('lag.*'
        ).filter( col('city').isNotNull() & col('city2').isNotNull()
        ).filter( col('city') != col('city2')
        ).groupBy('city', 'city2', 'freq'
        ).agg( lag_stats(f.sort_array(collect_list(struct('lag', 'p')))).alias('stats')
        ).select('city', 'city2', 'freq', 'stats.*'
        ).filter( col('count') >= 1
        ).sort(f.desc('count'), f.desc('lag50p')
        # ).agg(f.sum(col('lag') * col('p')).alias('mlag'), f.sum('p').alias('count')
        # ).filter( col('count') >= 10
                  # ).withColumn('mlag', col('mlag') / col('count')
        # ).sort(f.desc('count'), f.desc('mlag')
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
