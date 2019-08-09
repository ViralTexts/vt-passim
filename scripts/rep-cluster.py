from __future__ import print_function
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import arrays_zip, col, udf, collect_list, size, slice, \
    struct, sort_array, explode, datediff

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Get texts repeated in same series').getOrCreate()

    spark.read.load(sys.argv[1]) \
        .withColumn('date', col('date').cast('date')) \
        .groupBy('cluster', 'size', 'series', 'source') \
        .agg(sort_array(collect_list(struct('date', 'text'))).alias('reps')) \
        .filter(size('reps') > 1) \
        .withColumn('next', slice(col('reps'), 2, 100)) \
        .withColumn('rep', explode(arrays_zip('reps', 'next'))) \
        .filter(col('rep.next').isNotNull()) \
        .select('cluster', 'size', 'series', 'source',
                col('rep.reps').alias('prev'), col('rep.next').alias('next')) \
        .withColumn('lag', datediff(col('next.date'), col('prev.date'))) \
        .write.json(sys.argv[2])
    spark.stop()
