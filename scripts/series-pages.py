import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, size, struct, when, year)
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute page ranges for series',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Compute page ranges for series').getOrCreate()

    spark.read.load(config.inputPath, mergeSchema=True
        ).filter(col('year').isNotNull() & col('pp').isNotNull() & (col('pp') > 0)
        ).withColumn('sections', when(col('sections') <= 0, None).otherwise(col('sections'))
        ).groupBy('series', 'corpus', 'year', 'pp', 'sections', 'collation'
        ).count(
        ).sort('series', 'corpus', 'year', 'pp', 'sections', 'collation'
        ).coalesce(1
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
    
