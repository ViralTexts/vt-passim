import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, sort_array, struct, udf
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Page period',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    pages = spark.read.load(config.inputPath
                ).groupBy('book', 'pos', 'lang'
                ).agg(f.sum('length').alias('length')
                ).groupBy('book', 'pos'
                ).agg(f.max(struct('length', 'lang')).alias('top'), f.sum('length').alias('total')
                ).select('book', 'pos',
                         f.when((col('top')['length']*1.0/col('total') >= 0.7),
                                col('top')['lang']).otherwise(lit('und')).alias('lang'))

    pages.join(pages.select('book', (col('pos') + 1).alias('pos'), col('lang').alias('plang')),
               ['book', 'pos']
        ).filter(col('plang') != col('lang')
        ).groupBy('book', 'plang', 'lang'
        ).agg(f.min('pos').alias('begin'),
              f.max('pos').alias('end'),
              f.count('pos').alias('count')
        ).withColumn('prop', col('count')*2.0 / (col('end') - col('begin'))
        ).filter('(count > 20) AND (prop >= 0.5)'
#        ).filter("(plang <> 'und') AND (lang <> 'und')"
        ).groupBy('book'
        ).agg(f.max(struct('count', 'prop', 'plang', 'lang', 'begin', 'end')).alias('info')
        ).select('book', 'info.*'
        ).sort(f.desc('count'), f.desc('prop'), 'book'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
