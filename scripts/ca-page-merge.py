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

    file_stem = udf(lambda path: splitext(basename(path))[0] if path != None else None)
    make_url = udf(lambda series, date, ed, seq: f'https://www.loc.gov/resource/{series.replace("/lccn/","")}/{date}/ed-{ed}/?sp={seq}')
    make_iiif = udf(lambda fname: makeIIIF(fname))

    alto = spark.read.load(config.inputPath
            ).withColumn('sourceFrame', file_stem(translate('sourceFile', '\\_', '//'))
            ).withColumnRenamed('width', 'altoWidth'
            ).withColumnRenamed('height', 'altoHeight'
            ).drop('issue' #, 'text', 'regions'
            ).dropDuplicates(['batch', 'series', 'date', 'ed', 'seq', 'sourceFrame'])

    # alto.coalesce(200).write.save(config.outputPath + '/alto', mode='ignore')
    # alto = spark.read.load(config.outputPath + '/alto')

    mets = spark.read.json(config.metsPath).withColumnRenamed('file', 'metsFile'
                ).select('*', f.posexplode('pages')
                ).select('*', 'col.*', 'col.image.*'
                ).drop('pages', 'col', 'image'
                ).withColumn('frame', file_stem('file')
                ).withColumn('series', f.trim(f.lower('series'))
                ).withColumn('seq', col('seq').cast('int')
                ).withColumn('pp', col('pp').cast('int')
                ).withColumn('width', col('width').cast('int')
                ).withColumn('height', col('height').cast('int')
                ).withColumn('page_access', make_url('series', 'date', 'ed', 'seq'))

    mets.cache()

    # mets.write.save(config.outputPath + '/mets', mode='ignore')
    # mets = spark.read.load(config.outputPath + '/mets')

    mc = mets.filter(col('file').isNotNull()
            ).groupBy('batch', 'series', 'date', 'ed', 'seq'
            ).agg(f.count('metsFile').alias('mets'),
                  sort_array(collect_list(struct('frame', 'pos', 'metsFile'))).alias('frames'))

    ac = alto.groupBy('batch', 'series', 'date', 'ed', 'seq'
            ).agg(f.count('sourceFile').alias('alto'),
                  sort_array(collect_list('sourceFrame')).alias('sourceFrame'))
    
    jc = mc.join(ac, ['batch', 'series', 'date', 'ed', 'seq']
            ).withColumn('pair', explode(f.arrays_zip('frames', 'sourceFrame'))
            ).select('batch', 'series', 'date', 'ed', 'seq', 'pair.frames.*', 'pair.sourceFrame')

    linked = mets.join(jc.select('metsFile', 'pos', 'sourceFrame'),
                       ['metsFile', 'pos'], 'left_outer'
                ).withColumnRenamed('metsFile', 'issue')

    ## Don't deduplicate issues until here, since we want a 1-to-1 mets-alto match
    dedup = linked.withColumn('alto', col('sourceFrame').isNotNull().cast('int')
                ).groupBy('series', 'date', 'ed', 'issue'
                ).agg(f.sum('alto').alias('alto'), f.count('pos').alias('pp')
                ).groupBy('series', 'date', 'ed'
                ).agg((f.max(struct('alto', 'pp', 'issue'))['issue']).alias('issue'))

    linked.join(broadcast(dedup), ['series', 'date', 'ed', 'issue'], 'left_semi'
        ).join(alto, ['batch', 'series', 'date', 'ed', 'seq', 'sourceFrame'], 'left_outer'
        ).withColumn('id', f.concat('issue', lit('#pageModsBib'), (col('pos') + 1))
        ).withColumn('scale', f.coalesce((col('altoWidth')/col('width')).cast('int'), lit(1))
        ).withColumn('pages', f.array(struct(f.concat(lit('https://tile.loc.gov/storage-services/'), 'file').alias('id'),
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
        ).drop('altoWidth', 'altoHeight', 'dpi', 'file', 'regions', 'scale',
               'width', 'height'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
    
