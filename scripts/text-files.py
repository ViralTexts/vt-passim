from __future__ import print_function
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: text-files.py <input> <output>", file=sys.stderr)
        exit(-1)

    groupField = 'book'
    spark = SparkSession.builder.appName('Load Whole Text Files').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')
    spark.sparkContext.wholeTextFiles(sys.argv[1])\
        .map(lambda f: Row(id=f[0], text=f[1]))\
        .toDF()\
        .withColumn(groupField, col('id')) \
        .write.save(sys.argv[2])
    spark.stop()
