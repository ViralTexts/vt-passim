from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

def hathiRecord(r):
    return dict([(f["@name"], f["#VALUE"]) for f in r.field])

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: pretty-cluster.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Prettyprint Clusters")
    sqlContext = SQLContext(sc)

    raw = sqlContext.read.format('com.databricks.spark.xml') \
                         .options(rowTag='doc') \
                         .load(sys.argv[1])

    sqlContext.createDataFrame(raw.map(hathiRecord), samplingRatio=1) \
        .withColumn('seq', col('seq').cast('int')) \
        .withColumnRenamed('htid', 'book') \
        .withColumnRenamed('content', 'text') \
        .withColumnRenamed('year', 'date') \
        .repartition(200) \
        .write.save(sys.argv[2])
    
    sc.stop()
