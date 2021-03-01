from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, max

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: c19.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Select c19').getOrCreate()
    raw = spark.read.option('mergeSchema','true').load(sys.argv[1])
    raw.filter(col('date') < '1900'
               ).write.partitionBy('open', 'corpus').save(sys.argv[2])

    spark.stop()
