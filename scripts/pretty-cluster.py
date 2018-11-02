from __future__ import print_function

import sys
from re import sub

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, array_contains, explode, desc, concat_ws, coalesce

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
def formatURL(baseurl, corpus, id, p1x, p1y, p1w, p1h, p1dpi, p1id, dpi):
    if corpus == 'ca' and p1id != None:
        scale = p1dpi or dpi or 3
        return "https://chroniclingamerica.loc.gov%s/print/image_600x600_from_%d%%2C%d_to_%d%%2C%d/" \
            % (p1id, p1x/scale, p1y/scale, (p1x + p1w)/scale, (p1y + p1h)/scale)
    elif corpus == 'trove':
        return "http://trove.nla.gov.au/ndp/del/article/%s" % sub("^trove/", "", id)
    elif corpus == 'europeana':
        return 'http://data.theeuropeanlibrary.org/BibliographicResource/%s' % sub('^europeana/([^/]+).*$', '\\1', id)
    else:
        return baseurl

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: pretty-cluster.py <metadata> <input> <output> [<query>]", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Prettyprint Clusters').getOrCreate()

    outpath = sys.argv[3]
    (outputFormat, outputOptions) = guessFormat(outpath, "json")

    ## Should do more field renaming in meta to avoid clashing with fields in raw.
    meta = spark.read.json(sys.argv[1])\
            .dropDuplicates(['series'])\
            .withColumnRenamed('publisher', 'series_publisher') \
            .withColumnRenamed('title', 'series_title')

    
    constructURL = udf(lambda url, corpus, id, p1x, p1y, p1w, p1h, p1dpi, p1id, dpi: formatURL(url, corpus, id, p1x, p1y, p1w, p1h, p1dpi, p1id, dpi))

    imageLink = udf(lambda url, corpus: (url.replace('/print/', '/').rstrip('/') + '.jpg') if (url != None and corpus == 'ca') else None)
    thumbLink = udf(lambda image: image.replace('_600x600_', '_80x100_') if image != None else None)

    df = spark.read.load(sys.argv[2]) \
            .withColumn('title', coalesce('heading', 'title')) \
            .withColumnRenamed('lang', 'doc_lang')\
            .withColumn('p1x', col('pages')[0]['regions'][0]['coords']['x']) \
            .withColumn('p1y', col('pages')[0]['regions'][0]['coords']['y']) \
            .withColumn('p1w', col('pages')[0]['regions'][0]['coords']['w']) \
            .withColumn('p1h', col('pages')[0]['regions'][0]['coords']['h']) \
            .withColumn('p1seq', col('pages')[0]['seq']) \
            .withColumn('p1width', col('pages')[0]['width']) \
            .withColumn('p1height', col('pages')[0]['height']) \
            .withColumn('p1dpi', col('pages')[0]['dpi']) \
            .withColumn('p1id', col('pages')[0]['id']) \
            .drop('locs').drop('pages').drop('regions')\
            .join(meta, 'series', 'left_outer') \
            .withColumn('source', coalesce('source', 'series_title')) \
            .withColumn('publisher', coalesce('publisher', 'series_publisher')) \
            .drop('series_title', 'series_publisher') \
            .withColumn('url', constructURL('page_access', 'corpus', 'id', 'p1x', 'p1y', 'p1w', 'p1h', 'p1dpi', 'p1id', 'dpi')) \
            .withColumn('page_image', imageLink('url', 'corpus')) \
            .withColumn('page_thumb', thumbLink('page_image'))

    filtered = df.join(df.filter(sys.argv[4]).select('cluster').distinct(), 'cluster') \
               if len(sys.argv) >= 5 else df

    res = filtered.withColumn('lang', concat_ws(',', col('lang')))

    out = res.orderBy(desc('size'), 'cluster', 'date', 'id', 'begin') \
          if outputFormat != 'parquet' else res

    out.write.format(outputFormat).options(**outputOptions).save(outpath)

    spark.stop()
    
