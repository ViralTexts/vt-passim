import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, udf)
from pyspark.sql.functions import regexp_replace as rr
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='KYNP Metadata',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('idsPath', metavar='<path>', help='ids path')
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('KYNP Metadata').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    ids = spark.read.text(config.idsPath).toDF('issue')

    spark.read.json(config.inputPath
        ).na.drop(subset=['identifier']
        ).select(col('identifier').alias('issue'),
                 rr(rr(f.coalesce('serialurl', 'moreinfo'),
                       r'^.*(/lccn/[^/]+).*$', '$1'),
                    r'^(/lccn/)sn(.{10,})$', '$1$2').alias('series'),
                 'date'
        ).filter(col('series').rlike('^/lccn/')
        ).distinct(
        ).join(ids, ['issue'], 'left_semi'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
