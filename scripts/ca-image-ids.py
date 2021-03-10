from __future__ import print_function
import os, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, collect_set, concat_ws, input_file_name, \
    posexplode, regexp_replace, size, split, sort_array, udf

def idPrefix(batch, path):
    dirs = path.lower().split('/')
    if len(dirs) < 5:
        return ''
    issue = dirs[-2]
    if len(issue) < 9:
        return ''
    series = ([s for s in dirs[1:] if s.startswith('sn')] or [dirs[2]])[0]
    date = '-'.join([issue[0:4], issue[4:6], issue[6:8]])
    ed = 'ed-' + issue[8:10].lstrip('0')
    return '/'.join(['', 'ca', batch, series, date, ed])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: ca-image-ids.py <input> <output>', file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Scan CA manifests').getOrCreate()

    ## Spark already munges paths to use '/'
    dirname = udf(lambda path: path.split('/')[-2])

    id_prefix = udf(lambda batch, path: idPrefix(batch, path))
    
    spark.read.text(sys.argv[1]) \
        .withColumn('batch', dirname(input_file_name())) \
        .select(col('batch'), concat_ws('/', col('batch'),
                                        split('value', '\s+')[1]).alias('file')) \
        .filter(col('file').endswith('.jp2')) \
        .distinct() \
        .withColumn('prefix', id_prefix('batch', 'file')) \
        .withColumn('dir', regexp_replace('file', '/[^/]+$', '')) \
        .filter(col('prefix') != '') \
        .groupBy('prefix') \
        .agg(collect_set('dir').alias('dirs'), sort_array(collect_list('file')).alias('files')) \
        .filter(size('dirs') == 1) \
        .select('prefix', posexplode('files').alias('seq', 'file')) \
        .select(concat_ws('/seq-', 'prefix', col('seq') + 1).alias('id'), 'file') \
        .write.json(sys.argv[2])
    spark.stop()
