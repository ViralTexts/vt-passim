import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, sort_array, struct
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix dates',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('seriesPath', metavar='<series path>', help='series path')
    parser.add_argument('datePath', metavar='<date path>', help='date path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Fix dates').getOrCreate()

    seriesMap = spark.read.json(config.seriesPath)

    dateMap = spark.read.json(config.datePath)

    spark.read.load(config.inputPath
        ).join(f.broadcast(seriesMap), ['series'], 'left_outer'
        ).filter( (col('startdate').isNull() | (col('startdate') <= col('date'))) &
                  (col('enddate').isNull() | (col('date') <= col('enddate')))
        ).withColumn('series', f.coalesce('correct_series', 'series')
        ).drop('correct_series', 'startdate', 'enddate'
        ).join(f.broadcast(dateMap), ['series', 'date'], 'left_outer'
        ).withColumn('date', f.coalesce('correct_date', 'date')
        ).drop('correct_date'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
    
