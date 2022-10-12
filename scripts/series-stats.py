import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, size, struct, when, year)
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute yearly series statistics',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Compute yearly series statistics').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    raw = spark.read.load(config.inputPath, mergeSchema=True)

    datecover = raw.groupBy('series', 'date'
                    ).agg(f.min(struct(~col('open').cast('boolean'),
                                       col('corpus')))['corpus'].alias('corpus'))

    raw.join(f.broadcast(datecover), ['series', 'date', 'corpus'], 'left_semi'
        ).withColumn('pnum', coalesce((col('pos') + 1),
                                      f.array_max(col('pages')['seq']),
                                      col('seq')) + f.when(col('corpus') == 'ia', 1).otherwise(0)
        ).withColumn('chars', f.length('text')
        ).groupBy('series', 'corpus', 'issue', 'date', year('date').alias('year')
        ).agg(coalesce(f.max('pnum'), f.countDistinct('pageno')).alias('pp'),
              f.sum('chars').alias('chars'),
              f.sum(col('chars').isNull().cast('int')).alias('empties'),
              f.first('collation').alias('collation')
        ).filter(col('year').isNotNull()
        ).withColumn('sections', size(f.split('collation', '\+'))
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
