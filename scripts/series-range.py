import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, year)
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute date ranges for series',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Compute date ranges for series').getOrCreate()

    spark.read.load(config.inputPath, mergeSchema=True
        ).withColumn('date', col('date').cast('date')
        ).groupBy('series', 'corpus'
        ).agg(f.min('date').alias('startDig'),
              f.max('date').alias('endDig')
        ).sort('series', 'corpus'
        ).coalesce(1
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
    
