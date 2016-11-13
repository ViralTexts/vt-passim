from __future__ import print_function

import sys
from re import sub

from pyspark import SparkContext
from pyspark.sql import SQLContext
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

## This should be made obsolete by including links in document records.
def formatURL(url, corpus, id, regions):
    if corpus == 'ca' and regions != None and len(regions) > 0:
        r = regions[0]
        return "%s/print/image_600x600_from_%d%%2C%d_to_%d%%2C%d/" \
            % (url, r.x/3, r.y/3, (r.x + r.w)/3, (r.y + r.h)/3)
    elif corpus == 'trove':
        return "http://trove.nla.gov.au/ndp/del/article/%s" % sub("^trove/", "", id)
    else:
        return url

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: pretty-cluster.py <metadata> <input> <output> [<query>]", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Prettyprint Clusters")
    sqlContext = SQLContext(sc)

    outpath = sys.argv[3]
    (outputFormat, outputOptions) = guessFormat(outpath, "json")

    ## Should do more field renaming in meta to avoid clashing with fields in raw.
    meta = sqlContext.read.json(sys.argv[1])\
           .dropDuplicates(['series'])
    
    constructURL = udf(lambda url, corpus, id, regions: formatURL(url, corpus, id, regions))

    df = sqlContext.read.load(sys.argv[2]) \
        .withColumnRenamed('title', 'doc_title')\
        .withColumnRenamed('lang', 'doc_lang')\
        .withColumn('url', constructURL(col('page_access'), col('corpus'), col('id'), col('regions')))\
        .drop('locs').drop('pages').drop('regions')\
        .join(meta, 'series', 'left_outer')

    filtered = df.join(df.filter(sys.argv[4]).select('cluster').distinct(), 'cluster') \
               if len(sys.argv) >= 5 else df

    filtered.withColumn('lang', concat_ws(',', col('lang'))) \
            .orderBy(desc('size'), 'cluster', 'date', 'id', 'begin')\
            .write.format(outputFormat).options(**outputOptions).save(outpath)

    sc.stop()
    
