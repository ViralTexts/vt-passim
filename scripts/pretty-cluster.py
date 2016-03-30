from __future__ import print_function

import sys
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, array_contains, explode, desc
from pyspark.sql.types import StringType

def guessFormat(path, default="json"):
    if path.endswith(".json"):
        return ("json", {})
    elif path.endswith(".parquet"):
        return ("parquet", {})
    elif path.endswith(".csv"):
        return ("com.databricks.spark.csv", {'header': 'true'})
    else:
        return (default, {})

def formatURL(url, corpus, id, pages, regions):
    if corpus == 'ca':
        r = regions[0]
        return "%s/print/image_600x600_from_%d%%2C%d_to_%d%%2C%d/" \
            % (url, r.x/3, r.y/3, (r.x + r.w)/3, (r.y + r.h)/3)
    elif corpus == 'onb':
        return "%s&seite=%s" % (sub("&amp;", "&", url), pages[0])
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
           .withColumnRenamed('lang', 'series_lang')\
           .withColumnRenamed('id', 'series').dropDuplicates(['series'])
    
    df = sqlContext.read.load(sys.argv[2])
    
    if len(sys.argv) >= 5:
        df.registerTempTable("clusters")
        print(sys.argv[4])
        raw = df.join(sqlContext.sql(sys.argv[4]).select("cluster").distinct(), 'cluster')
    else:
        raw = df

    h = HTMLParser.HTMLParser()
    removeTags = udf(lambda s: h.unescape(sub("</?[A-Za-z][^>]*>", "", s)), StringType())
    constructURL = udf(lambda url, corpus, id, pages, regions: formatURL(url, corpus, id, pages, regions),
                       StringType())

    raw\
        .withColumnRenamed("title", "heading")\
        .withColumn("url", constructURL(raw.url, raw.corpus, raw.id, raw.pages, raw.regions))\
        .drop("locs")\
        .drop("pages")\
        .drop("regions")\
        .withColumn("text", removeTags(raw.text))\
        .join(meta, 'series', 'left_outer')\
        .orderBy(desc("size"), "cluster", "date", "id", "begin")\
        .write.format(outputFormat).options(**outputOptions).save(outpath)

    sc.stop()
    
