import argparse
from urllib.parse import urlparse
from os.path import basename, splitext
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import (broadcast, col, collect_list, explode, lit, size, udf, struct,
                                   sort_array, translate, when)
import pyspark.sql.functions as f

def makeURL(issue, series, date, ed, seq):
    res = urlparse(issue)
    return f'{res.scheme}://{res.netloc}{series}/{date}/ed-{ed}/seq-{seq}/'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merge METS and Alto',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--urlocr', action='store_true', help='Use URL for OCR path.')
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('metsPath', metavar='<path>', help='path to METS data')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Merge METS and Alto').getOrCreate()

    make_url = udf(lambda issue, series, date, ed, seq: makeURL(issue, series, date, ed, seq))

    make_alto = udf(lambda ocrFile, page_access: (page_access + 'ocr.xml') if config.urlocr else ocrFile)

    alto = spark.read.load(config.inputPath
            ).withColumnRenamed('width', 'altoWidth'
            ).withColumnRenamed('height', 'altoHeight'
            ).drop('sourceFile'
            ).dropDuplicates(['batch', 'alto'])

    mets = spark.read.json(config.metsPath).withColumnRenamed('file', 'issue'
                ).select('*', f.posexplode('pages')
                ).select('*', 'col.*', 'col.image.*'
                ).drop('pages', 'col', 'image'
                ).withColumn('series', f.trim(f.lower('series'))
                ).withColumn('seq', col('seq').cast('int')
                ).withColumn('page_access', make_url('issue', 'series', 'date', 'ed', 'seq')
                ).withColumn('ocrFile', f.regexp_replace('file', r'\.[^/\.]+$', '.xml')
                ).withColumn('alto', make_alto('ocrFile', 'page_access')
                ).drop('ocrFile')

    mets.join(alto, ['batch', 'alto'], 'left_outer'
        ).withColumn('id', f.concat('issue', lit('#pageModsBib'), (col('pos') + 1))
        ).withColumn('pages', f.array(struct(col('file').alias('id'),
                                             'seq',
                                             col('altoWidth').alias('width'),
                                             col('altoHeight').alias('height'),
                                             'dpi', 'regions'))
        ).drop('alto', 'altoWidth', 'altoHeight', 'dpi', 'file', 'regions'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
