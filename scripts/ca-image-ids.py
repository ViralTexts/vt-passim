from __future__ import print_function
import os, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, concat_ws, input_file_name, \
    posexplode, split, sort_array, udf

def idPrefix(path):
    dirs = path.split('/')
    issue = dirs[4]
    if len(issue) != 10:
        return ''
    date = '-'.join([issue[0:4], issue[4:6], issue[6:8]])
    ed = 'ed-' + issue[8:10].lstrip('0')
    return '/'.join(['', 'lccn', dirs[2], date, ed])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: ca-image-ids.py <input> <output>', file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Scan CA manifests').getOrCreate()

    ## Spark already munges paths to use '/'
    dirname = udf(lambda path: path.split('/')[-2])

    id_prefix = udf(lambda path: idPrefix(path))
    
    spark.read.text(sys.argv[1]) \
        .select(concat_ws('/', dirname(input_file_name()),
                          split('value', '\s+')[1]).alias('file')) \
        .filter(col('file').endswith('.jp2')) \
        .withColumn('prefix', id_prefix('file')) \
        .filter(col('prefix') != '') \
        .groupBy('prefix') \
        .agg(sort_array(collect_list('file')).alias('files')) \
        .select('prefix', posexplode('files').alias('seq', 'file')) \
        .select(concat_ws('/seq-', 'prefix', col('seq')+1).alias('id'), 'file') \
        .write.json(sys.argv[2])
    spark.stop()
