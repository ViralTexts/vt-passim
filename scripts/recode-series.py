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

    input = spark.read.load(config.inputPath).withColumnRenamed('series', 'series2')

    input.join(series, [input.series2 == series.series2,
                        col('date') >= col('start'), col('date') <= col('end')],
               'left_outer'
        ).drop(series.series2   # duplication from non-equi-join
        ).withColumn('series', f.coalesce('series', 'series2')
        ).drop('series2', 'start', 'end'
        ).write.save(config.outputPath)

    spark.stop()
    
