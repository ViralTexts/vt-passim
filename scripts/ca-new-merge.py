import argparse
from re import sub
from os.path import basename, splitext
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import (broadcast, col, collect_list, explode, lit, size, udf, struct,
                                   sort_array, translate, when)
import pyspark.sql.functions as f

def makeIIIF(fname):
    if fname == None or fname == '':
        return None
    return 'https://tile.loc.gov/image-services/iiif/' + sub(r'\.jp2$','',fname).replace('/',':')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merge METS and Alto',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('metsPath', metavar='<path>', help='path to METS data')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Merge METS and Alto').getOrCreate()

    make_url = udf(lambda series, date, ed, seq: f'https://www.loc.gov/resource/{series.replace("/lccn/","")}/{date}/ed-{ed}/?sp={seq}')
    make_iiif = udf(lambda fname: makeIIIF(fname))

    alto = spark.read.load(config.inputPath
               ).filter(col('text') != 'fubar'
               ).select('alto', 'batch', 'dpi', 'regions', 'text',
                        col('width').alias('altoWidth'), col('height').alias('altoHeight')
               ).dropDuplicates(['alto'])

    batches = alto.select('batch').distinct()
    batches.cache()

    spark.read.json(config.metsPath
        ).join(broadcast(batches), ['batch'], 'left_semi'
        ).withColumnRenamed('file', 'issue'
        ).select('*', f.posexplode('pages')
        ).select('*', 'col.*', 'col.image.*'
        ).drop('pages', 'col', 'image'
        ).withColumn('series', f.trim(f.lower('series'))
        ).withColumn('seq', col('seq').cast('int')
        ).withColumn('pp', col('pp').cast('int')
        ).withColumn('width', col('width').cast('int')
        ).withColumn('height', col('height').cast('int')
        ).withColumn('viewer', make_url('series', 'date', 'ed', 'seq')
        ).withColumn('id', f.concat('issue', lit('#pageModsBib'), (col('pos') + 1))
        ).withColumn('iiif', make_iiif('file')
        ).withColumn('file', f.concat(lit('https://tile.loc.gov/storage-services/'), 'file')
        ).withColumn('alto', f.regexp_replace('file', r'jp2$', 'xml')
        ).join(alto.drop('batch'), ['alto'], 'left_outer'
        ).withColumn('scale', f.coalesce((col('altoWidth')/col('width')), lit(1))
        ).withColumn('pages', f.array(struct(col('file').alias('id'), 'iiif', 'viewer',
                                             'seq', 'width', 'height',
                                             col('dpi').cast('int').alias('dpi'),
                                             f.transform('regions',
                                                lambda r: struct(r.start.cast('int').alias('start'),
                                                                 r.length.cast('int').alias('length'),
                                                                 struct(
                                                (r.coords.x/col('scale')).cast('int').alias('x'),
                                                (r.coords.y/col('scale')).cast('int').alias('y'),
                                                (r.coords.w/col('scale')).cast('int').alias('w'),
                                                (r.coords.h/col('scale')).cast('int').alias('h'),
                                                (r.coords.b/col('scale')).cast('int').alias('b')
                                                                 ).alias('coords'))).alias('regions')))
        ).drop('alto', 'dpi', 'regions', 'altoWidth', 'altoHeight', 'scale',
               'file', 'iiif', 'width', 'height', 'viewer'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
