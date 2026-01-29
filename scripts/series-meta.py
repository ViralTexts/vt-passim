from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, collect_list, sort_array)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: series-meta.py <input> <corpora> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Collect series metadata').getOrCreate()

    data = spark.read.json(sys.argv[1]
                ).withColumn('lang', concat_ws(';', 'lang')
                ).join(spark.read.csv(sys.argv[2], header=True
                            ).groupBy('series'
                            ).agg(concat_ws(';',
                                            collect_list('corpus')).alias('corpora')),
                       'series', 'left_outer'
                ).select('series', 'title', 'lang', 'publisher', 'placeOfPublication',
                         'corpora', 'coverage'
                ).coalesce(1
                ).sort('series')

    data.write.json(sys.argv[3], mode='overwrite')

    data.write.csv(sys.argv[3] + '.csv', header=True, escape='"', mode='overwrite')

    spark.stop()
