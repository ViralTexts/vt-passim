import argparse, re
from re import sub
import urllib.parse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, get, udf, array_contains, explode, desc,
                                   concat_ws, coalesce, lit)
import pyspark.sql

def guessFormat(path, default="json"):
    if path.endswith(".json"):
        return ("json", {'compression': 'gzip'})
    elif path.endswith(".parquet"):
        return ("parquet", {})
    elif path.endswith(".csv"):
        return ("csv", {'header': 'true', 'compression': 'gzip', 'escape': '"'})
    else:
        return (default, {})

def imageLink(p1iiif, p1x, p1y, p1w, p1h, p1width, p1height):
    if p1iiif != None and p1width != None and p1height != None and p1x != None:
        return '%s/pct:%f,%f,%f,%f/full/0/default.jpg' \
            % (p1iiif, 100 * p1x/p1width, 100*p1y/p1height, 100*p1w/p1width, 100*p1h/p1height)
    else:
        return None

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='Prettyprint clusters')
    argparser.add_argument('-p', '--places', help='place data')
    argparser.add_argument('metaPath', help='Metadata path')
    argparser.add_argument('inputPath', help='Input path')
    argparser.add_argument('outputPath', help='Output path')
    argparser.add_argument('filter', nargs='?', default=None, help='Filter reprints')
    config = argparser.parse_args()

    spark = SparkSession.builder.appName('Prettyprint Clusters').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')
    
    (outputFormat, outputOptions) = guessFormat(config.outputPath, 'json')

    ## Should do more field renaming in meta to avoid clashing with fields in raw.
    meta = spark.read.json(config.metaPath
                ).dropDuplicates(['series']
                ).withColumnRenamed('publisher', 'series_publisher'
                ).withColumnRenamed('placeOfPublication', 'series_placeOfPublication'
                ).withColumnRenamed('title', 'series_title')

    if config.places:
        meta = meta.join(
            spark.read.csv(config.places, header=True).withColumnRenamed('label', 'city'),
            ['coverage'], 'left_outer')
    
    image_link = udf(lambda p1iiif, p1x, p1y, p1w, p1h, p1width, p1height: imageLink(p1iiif, p1x, p1y, p1w, p1h, p1width, p1height))
    thumb_link = udf(lambda image: image.replace('/full/', '/!80,100/') if image != None else None)

    raw = spark.read.load(config.inputPath)
    cols = set(raw.columns)
    for f in ['source', 'publisher', 'placeOfPublication', 'viewer', 'page_access', 'title']:
        if f not in cols:
            raw = raw.withColumn(f, lit(None))

    df = raw.withColumnRenamed('lang', 'doc_lang'
           ).withColumn('src', get('src', 0)
           ).withColumn('sbegin', col('src')['begin']
           ).withColumn('send', col('src')['end']
           ).withColumn('src', col('src')['uid']
           ).withColumn('p1x', get('pages', 0)['regions'][0]['coords']['x']
           ).withColumn('p1y', get('pages', 0)['regions'][0]['coords']['y']
           ).withColumn('p1w', get('pages', 0)['regions'][0]['coords']['w']
           ).withColumn('p1h', get('pages', 0)['regions'][0]['coords']['h']
           ).withColumn('p1seq', get('pages',  0)['seq']
           ).withColumn('p1width', get('pages', 0)['width']
           ).withColumn('p1height', get('pages', 0)['height']
           ).withColumn('p1dpi', get('pages', 0)['dpi']
           ).withColumn('p1id', get('pages', 0)['id']
           ).withColumn('p1iiif', get('pages', 0)['iiif']
           ).withColumn('url', coalesce('viewer', get('pages', 0)['viewer'], 'page_access')
           ).drop('locs', 'pages', 'regions', 'sections', 'page_access', 'viewer'
           ).join(meta, 'series', 'left_outer'
           ).withColumn('source', coalesce('source', 'series_title')
           ).withColumn('publisher', coalesce('publisher', 'series_publisher')
           ).withColumn('placeOfPublication',
                        coalesce('placeOfPublication', 'series_placeOfPublication')
           ).drop('series_title', 'series_publisher', 'series_placeOfPublication'
           ).withColumn('page_image', image_link('p1iiif', 'p1x', 'p1y', 'p1w', 'p1h',
                                                 'p1width', 'p1height')
           ).withColumn('page_thumb', thumb_link('page_image'))

    filtered = df.join(df.filter(config.filter).select('cluster').distinct(), 'cluster') \
               if config.filter else df

    res = filtered.withColumn('lang', concat_ws(',', col('lang')))

    out = res.orderBy(desc('size'), 'cluster', 'date', 'id', 'begin') \
          if outputFormat != 'parquet' else res

    out.write.format(outputFormat).options(**outputOptions
        ).save(config.outputPath, mode='overwrite')

    spark.stop()
    
