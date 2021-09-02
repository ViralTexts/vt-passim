from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, collect_list, sort_array)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: series-meta.py <input> <corpora> <coverage> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Collect series metadata').getOrCreate()

    data = spark.read.json(sys.argv[1]
                ).withColumn('lang', concat_ws(';', 'lang')
                ).join(spark.read.json(sys.argv[2]
                            ).groupBy('series'
                            ).agg(concat_ws(';',
                                            collect_list('corpus')).alias('corpus')),
                       'series', 'left_outer'
                ).join(spark.read.json(sys.argv[3]), 'coverage', 'left_outer'
                ).select('series', 'title', 'lang', 'publisher', 'placeOfPublication',
                         'subcollection', 'corpus', 'coverage', 'lon', 'lat'
                ).coalesce(1
                ).sort('series')

    data.write.json(sys.argv[4])

    data.write.csv(sys.argv[4] + '.csv', header=True, escape='"')

    spark.stop()
