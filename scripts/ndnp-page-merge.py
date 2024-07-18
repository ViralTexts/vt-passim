import argparse, re
from urllib.parse import quote, urlparse
from os.path import basename, splitext
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import (broadcast, col, collect_list, explode, lit, size, udf, struct,
                                   sort_array, translate, when)
import pyspark.sql.functions as f

def makeURL(issue, series, date, ed, seq):
    res = urlparse(issue)
    return f'{res.scheme}://{res.netloc}{series}/{date}/ed-{ed}/seq-{seq}/'

def makeIIIF(fname):
    if fname == None or fname == '':
        return None
    domain, file = re.split(r'/data/batches/', fname)
    mid = '/images/iiif/' if domain.find('panewsarchive') == -1 else '/iiif/'
    return domain + mid + quote(file, safe='')

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

    make_iiif = udf(lambda fname: makeIIIF(fname))

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
                ).withColumn('pp', col('pp').cast('int')
                ).withColumn('width', col('width').cast('int')
                ).withColumn('height', col('height').cast('int')
                ).withColumn('page_access', make_url('issue', 'series', 'date', 'ed', 'seq')
                ).withColumn('ocrFile', f.regexp_replace('file', r'\.[^/\.]+$', '.xml')
                ).withColumn('alto', make_alto('ocrFile', 'page_access')
                ).drop('ocrFile')

    if ('sections' in mets.columns) and (mets.filter(size('sections') > 0).count() == 0):
        mets = mets.drop('sections')

    mets.join(alto, ['batch', 'alto'], 'left_outer'
        ).withColumn('id', f.concat('issue', lit('#pageModsBib'), (col('pos') + 1))
        ).withColumn('scale', f.coalesce((col('altoWidth')/col('width')).cast('int'), lit(1))
        ).withColumn('pages', f.array(struct(col('file').alias('id'),
                                             make_iiif('file').alias('iiif'),
                                             'seq', 'width', 'height', 'dpi',
                                f.transform('regions',
                                            lambda r: r.withField('coords', struct(
                                                (r.coords.x/col('scale')).cast('int').alias('x'),
                                                (r.coords.y/col('scale')).cast('int').alias('y'),
                                                (r.coords.w/col('scale')).cast('int').alias('w'),
                                                (r.coords.h/col('scale')).cast('int').alias('h'),
                                                (r.coords.b/col('scale')).cast('int').alias('b'))
                                                                            )).alias('regions')))
        ).drop('alto', 'altoWidth', 'altoHeight', 'dpi', 'file', 'regions', 'scale',
               'width', 'height'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
