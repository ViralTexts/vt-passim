import argparse, glob, os, re, sys
from re import sub
import re
from unicodedata import normalize, combining
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (array, col, collect_list, explode, length, lit, regexp_replace,
                                   inline, size, sort_array, split, struct, translate, udf, when)
import pyspark.sql.functions as f

def lineLocs(lines, locs):
    iloc = 0
    res = []
    for line in lines:
        end = line.begin + len(line.text.rstrip())
        while iloc < len(locs) and (locs[iloc].start + locs[iloc].length) <= line.begin:
            iloc += 1
        cur = []
        while (iloc < len(locs)) and (locs[iloc].start < end):
            cur.append(locs[iloc].loc)
            iloc += 1
        if iloc > 0:
            iloc -= 1
        if len(cur) < 1:
            cur = None
        res.append((line.begin, cur))
        
    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collate all pairs to ref alignment',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('metaPath', metavar='<meta path>', help='meta path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    meta = spark.read.csv(config.metaPath, header=True, escape='"')

    raw = spark.read.load(config.inputPath)

    wits = raw.select('id', 'work', 'pos', size('lines').alias('nlines'),
                      explode('lines').alias('line')
             ).select('id', 'work', 'pos', 'nlines',
                      length('line.text').alias('llen'), col('line.begin'),
                      explode('line.wits').alias('wit')
             ).withColumn('slen', length(translate('wit.alg', '-', ''))
             ).withColumn('dlen', length(translate('wit.alg2', '-', ''))
             ).filter(col('dlen') == col('llen')
             ).filter(f.try_divide('wit.matches', f.greatest('slen', 'dlen')) > 0.5
             ).withColumn('page', regexp_replace('wit.id', r'^.+[/_](\d+)$', '$1')
             ).withColumn('book', regexp_replace('wit.id', r'[/_]\d+$', '')
             ).withColumn('page', when(f.regexp('page', lit(r'^\d+$')), col('page').cast('int')
                                       ).otherwise(lit(0)))

    eds = wits.groupBy('id', 'work', 'pos', 'nlines', 'book'
             ).agg(f.countDistinct('begin').alias('hits'),
                   f.min(struct('begin', 'page'))['page'].alias('minp'),
                   f.max(struct('begin', 'page'))['page'].alias('maxp')
             ).withColumn('cover', col('hits') / col('nlines')
             ).filter(col('cover') > 0.5
             ).join(meta.select('book', 'author', 'title', 'language_gen', 'date1_src'),
                    ['book'], 'left_outer')

    eds.sort('date1_src').write.json('eds.json', mode='overwrite')

    line_locs = udf(lambda lines, locs: lineLocs(lines, locs),
                    'array<struct<begin: int, locs: array<string>>>')

    locs = raw.select('work', 'pos', inline(line_locs('lines', 'locs')))

    wits.join(eds.select('id', 'book', 'date1_src'), ['id', 'book']
       ).groupBy('id', 'work', 'pos', 'begin', 'llen'
       ).agg(sort_array(collect_list(struct('date1_src', 'book', 'page', 'wit.begin',
                                            translate('wit.alg', '\u2010-', '-').alias('text'),
                                            # 'wit.alg2'
                                            ))).alias('wits')
       ).join(locs, ['work', 'pos', 'begin'], 'right_outer'
       ).select('work', 'pos', 'begin', 'llen', 'locs', 'wits'
       ).sort('work', 'pos', 'begin'
       ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
