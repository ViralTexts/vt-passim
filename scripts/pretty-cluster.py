from __future__ import print_function

import sys
from re import sub

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, array_contains, explode, desc, concat_ws

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
def formatURL(baseurl, corpus, id, pages):
    if corpus == 'ca' and pages != None and len(pages) > 0:
        c = pages[0]['regions'][0]['coords']
        pid = pages[0]['id']
        scale = pages[0]['dpi'] or 3
        return "http://chroniclingamerica.loc.gov%s/print/image_600x600_from_%d%%2C%d_to_%d%%2C%d/" \
            % (pid, c.x/scale, c.y/scale, (c.x + c.w)/scale, (c.y + c.h)/scale)
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
           .dropDuplicates(['series'])
    
    constructURL = udf(lambda url, corpus, id, regions: formatURL(url, corpus, id, regions))

    df = spark.read.load(sys.argv[2]) \
        .withColumnRenamed('title', 'doc_title')\
        .withColumnRenamed('lang', 'doc_lang')\
        .withColumn('url', constructURL(col('page_access'), col('corpus'), col('id'), col('pages'))) \
        .withColumn('p1x', col('pages')[0]['regions'][0]['coords']['x']) \
        .withColumn('p1y', col('pages')[0]['regions'][0]['coords']['y']) \
        .withColumn('p1w', col('pages')[0]['regions'][0]['coords']['w']) \
        .withColumn('p1h', col('pages')[0]['regions'][0]['coords']['h']) \
        .withColumn('p1seq', col('pages')[0]['seq']) \
        .withColumn('p1width', col('pages')[0]['width']) \
        .withColumn('p1height', col('pages')[0]['height']) \
        .withColumn('p1dpi', col('pages')[0]['dpi']) \
        .drop('locs').drop('pages').drop('regions')\
        .join(meta, 'series', 'left_outer')

    filtered = df.join(df.filter(sys.argv[4]).select('cluster').distinct(), 'cluster') \
               if len(sys.argv) >= 5 else df

    res = filtered.withColumn('lang', concat_ws(',', col('lang')))

    out = res.orderBy(desc('size'), 'cluster', 'date', 'id', 'begin') \
          if outputFormat != 'parquet' else res

    out.write.format(outputFormat).options(**outputOptions).save(outpath)

    spark.stop()
    
