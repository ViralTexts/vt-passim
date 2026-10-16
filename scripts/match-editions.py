import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import btrim, col, explode, concat, length, lit, struct, when
import pyspark.sql.functions as f

def good_line(x):
    return ((length(x['text']) >= 5) & (length(x['text']) <= 500) & x['text'].endswith('\n'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Match editions from docwise',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('metaPath', metavar='<path>', help='meta path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    meta = spark.read.json(config.metaPath).withColumn('gid', f.xxhash64('book'))

    raw = spark.read.load(config.inputPath)

    pages = raw.groupBy(col('book').alias('book2')).agg(f.count('id').alias('pages'))

    raw.select(col('id').alias('id2'), col('book').alias('book2'),
               f.filter('lines', good_line).alias('lines')
      ).select('id2', 'book2', f.size('lines').alias('nl2'), explode('lines').alias('line')
      ).filter(col('nl2') >= 10
      ).select('id2', 'book2', 'nl2', 'line.text', explode('line.wits').alias('wit')
      ).filter((col('wit.matches') / length('text')) > 0.5
      ).withColumn('match', (col('wit.text').endswith('\n') &
                             ~btrim('wit.text', lit('\n')).contains('\n')).cast('int')
      ).groupBy('id2', 'book2', 'nl2', 'wit.id', 'wit.gid'
      ).agg(f.sum('match').alias('matches'), f.count('match').alias('nl')
      ).withColumn('prop2', col('matches') / col('nl2')
      ).withColumn('prop', col('matches') / col('nl')
      ).groupBy('id2', 'book2', 'gid'
      ).agg(f.max(struct('nl', 'prop2', 'prop')).alias('info') # get best page match
      ).select('id2', 'book2', 'gid', 'info.*'
      ).withColumn('mpage',
                   ((col('nl') >= 10) & (col('prop2') >= 0.5) & (col('prop') >= 0.8)).cast('int')
      ).groupBy('book2', 'gid'
      ).agg(f.sum('mpage').alias('matches'), f.count('mpage').alias('cand'),
            f.mean('prop2').alias('pp2')
      ).join(pages, 'book2'
      ).join(meta, 'gid'
      ).join(meta.toDF(*[f + '2' for f in meta.columns]), 'book2'
      ).withColumn('prop', col('matches') / col('pages')
      ).sort(f.desc('prop'), 'book2'
      ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
