import argparse, re
from re import sub
import urllib.parse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, udf, array_contains, explode, desc,
                                   concat_ws, coalesce, lit)

def guessFormat(path, default="json"):
    if path.endswith(".json"):
        return ("json", {'compression': 'gzip'})
    elif path.endswith(".parquet"):
        return ("parquet", {})
    elif path.endswith(".csv"):
        return ("csv", {'header': 'true', 'compression': 'gzip', 'escape': '"'})
    else:
        return (default, {})

## Map article/page records and coordinate information to links
def formatURL(baseurl, corpus, id, p1id):
    if baseurl == None and corpus == 'ia' and p1id != None:
        return 'https://archive.org/details/%s/page/n%s/mode/1up?view=theater' \
            % (id, sub(r'^.*_0*(\d+).jp2$', r'\1', p1id))
    elif corpus == 'ddd':
        return sub(r':alto$', '', baseurl)
    elif corpus == 'trove':
        return "http://trove.nla.gov.au/ndp/del/article/%s" % sub("^trove/", "", id)
    elif corpus == 'europeana':
        return 'http://data.theeuropeanlibrary.org/BibliographicResource/%s' % sub('^europeana/([^/]+).*$', '\\1', id)
    else:
        return baseurl

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
    
    constructURL = udf(lambda url, corpus, id, p1id: formatURL(url, corpus, id, p1id))

    image_link = udf(lambda p1iiif, p1x, p1y, p1w, p1h, p1width, p1height: imageLink(p1iiif, p1x, p1y, p1w, p1h, p1width, p1height))
    thumb_link = udf(lambda image: image.replace('/full/', '/!80,100/') if image != None else None)

    raw = spark.read.load(config.inputPath)
    cols = set(raw.columns)
    for f in ['source', 'publisher', 'placeOfPublication', 'heading', 'page_access', 'title']:
        if f not in cols:
            raw = raw.withColumn(f, lit(None))

    df = raw.withColumn('title', coalesce('heading', 'title')
           ).withColumnRenamed('lang', 'doc_lang'
           ).withColumn('sbegin', col('src')[0]['begin']
           ).withColumn('send', col('src')[0]['end']
           ).withColumn('src', col('src')[0]['uid']
           ).withColumn('p1x', col('pages')[0]['regions'][0]['coords']['x']
           ).withColumn('p1y', col('pages')[0]['regions'][0]['coords']['y']
           ).withColumn('p1w', col('pages')[0]['regions'][0]['coords']['w']
           ).withColumn('p1h', col('pages')[0]['regions'][0]['coords']['h']
           ).withColumn('p1seq', col('pages')[0]['seq']
           ).withColumn('p1width', col('pages')[0]['width']
           ).withColumn('p1height', col('pages')[0]['height']
           ).withColumn('p1dpi', col('pages')[0]['dpi']
           ).withColumn('p1id', col('pages')[0]['id']
           ).withColumn('p1iiif', col('pages')[0]['iiif']
           ).drop('locs', 'pages', 'regions', 'sections'
           ).join(meta, 'series', 'left_outer'
           ).withColumn('source', coalesce('source', 'series_title')
           ).withColumn('publisher', coalesce('publisher', 'series_publisher')
           ).withColumn('placeOfPublication',
                        coalesce('placeOfPublication', 'series_placeOfPublication')
           ).drop('series_title', 'series_publisher', 'series_placeOfPublication'
           ).withColumn('url', constructURL('page_access', 'corpus', 'id', 'p1id')
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
    
