import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, size, struct, when, year)
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute yearly series output',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Compute yearly series output').getOrCreate()

    spark.read.load(config.inputPath, mergeSchema=True
        ).groupBy('series', 'year'
        ).agg(f.count('issue').alias('issues'),
              f.min('date').alias('startDig'), f.max('date').alias('endDig'),
              f.datediff(f.max('date'), f.min('date')).alias('days'),
              f.sum('pp').alias('pp'),
              f.sum('chars').alias('chars'),
              f.sum('empties').alias('empties'),
              f.format_string('%.04g', f.stddev('chars')).alias('sdchars')
        ).sort('series', 'year'
        ).coalesce(1
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
    
