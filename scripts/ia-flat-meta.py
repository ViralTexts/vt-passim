from __future__ import print_function
import sys
from pyspark.sql import SparkSession

def flattenRow(r):
    res = dict()
    if r['str'] != None:
        for elem in r['str']:
            res[elem['_name']] = elem['_VALUE']
    if r['arr'] != None:
        for elem in r['arr']:
            res[elem['_name']] = '; '.join(elem['str'])
    return res

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: %s <input> <output>" % sys.argv[0], file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('IA Metadata').getOrCreate()
    raw = spark.read.format('com.databricks.spark.xml').option('rowTag', 'doc').load(sys.argv[1])

    spark.createDataFrame(raw.rdd.map(flattenRow), samplingRatio=1).write.save(sys.argv[2])
    spark.stop()
    
    
