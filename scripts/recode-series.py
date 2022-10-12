import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, sort_array, struct
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Recode series',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('seriesPath', metavar='<series path>', help='series path')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Recode series').getOrCreate()

    series = spark.read.json(config.seriesPath
                ).groupBy('series2', 'series'
                ).agg(f.min('start').alias('start'), f.max('end').alias('end'))

    spark.read.load(config.inputPath).withColumnRenamed('series', 'series2'
        ).join(series, ['series2'], 'left_outer'
        ).filter( (col('start').isNull() | (col('start') <= col('date'))) &
                  (col('end').isNull() | (col('date') <= col('end')))
        ).withColumn('series', f.coalesce('series', 'series2')
        ).drop('series2', 'start', 'end'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
    
