from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import translate

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: vac-load.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Load VAC JSON').getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')
    spark.read.json(sys.argv[1]) \
        .withColumnRenamed('author', 'creator') \
        .withColumn('text', translate('text', '\r', '\n')) \
        .repartition(50) \
        .write.save(sys.argv[2])
    spark.stop()
