import argparse
from unicodedata import normalize
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, length, lit, sort_array, struct, udf
import pyspark.sql.functions as f

def catLocs(recs):
    text = ''
    locs = []
    for r in recs:
        locs.append((r.loc, len(text), len(r.text)))
        text += r.text
    return (text, locs)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import CTS Records',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--dedup', action='store_true', help='Retain one edition per work.')
    parser.add_argument('--top-split', action='store_true', help='Split works on top chunk.')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Import CTS Records').getOrCreate()

    nfd_norm = udf(lambda s: normalize('NFD', s))
    cat_locs = udf(lambda locs: catLocs(locs),
                   'struct<text: string, locs: array<struct<loc: string, start: int, length: int>>>')
    raw = spark.read.json(config.inputPath
              ).filter(f.rlike('book', lit(r'-grc')) | f.rlike('book', lit(r'-lat')))
    if config.dedup:
        eds = raw.groupBy('book'
                ).count(
                ).groupBy(f.regexp_replace('book', r'\.[^\.]+$', '').alias('work')
                ).agg(f.max(struct('count', 'book')).alias('info')
                ).select('work', col('info.book').alias('book'))
        raw = raw.join(eds, ['book'], 'left_semi')
        
    res = raw.filter(col('text').isNotNull()
            ).filter(f.rlike('book', lit(r'-grc')) | f.rlike('book', lit(r'-lat'))
            ).withColumn('text', nfd_norm('text'))
    if config.top_split:
        res = res.withColumn('parts', f.split(f.split('loc', ':')[4], r'\.')
                ).withColumn('top', f.when(((f.size('parts') >= 2) | ((f.size('parts') >= 1) & (col('book') == 'urn:cts:latinLit:stoa0119.stoa003.opp-lat1'))) & (f.size('parts') < 100),
                                           f.concat(f.lit(':'), col('parts')[0])
                                           ).otherwise(f.lit(''))
                ).groupBy(col('book'), f.concat('book', 'top').alias('id'))
    else:
        res = res.groupBy(col('book'), col('book').alias('id'))

    res.agg(cat_locs(sort_array(collect_list(struct('seq', 'text', 'loc')))).alias('loc'),
            f.min('seq').cast('int').alias('pos')
      ).select('id', 'book', 'pos', 'loc.*'
      ).filter(length('text')/f.size('locs') < 100000
      ).sort('id'
      ).write.save(config.outputPath)

    spark.stop()
