from __future__ import print_function

import sys
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, array_contains, explode, desc
from pyspark.sql.types import StringType

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
    if len(sys.argv) != 4:
        print("Usage: pretty-cluster.py <metadata> <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Parquet Load")
    sqlContext = SQLContext(sc)

    # outputFormat = "com.databricks.spark.csv"
    # outputOptions = {'header': 'true'}
    outputFormat = "json"
    outputOptions = {}
    
    meta = sqlContext.read.json(sys.argv[1])

    raw = sqlContext.read.load(sys.argv[2])

    ## There aren't many series.  It'll be cleaner to use a closure instead of a join
    stitle = dict(meta.select(explode(meta.publication_names.sn).alias("series"),
                             meta.master_name)\
                  .unionAll(meta.select(meta.id.alias("series"), meta.master_name))\
                  .distinct().collect())

    h = HTMLParser.HTMLParser()
    removeTags = udf(lambda s: h.unescape(sub("</?[A-Za-z][^>]*>", "", s)), StringType())
    constructURL = udf(lambda url, corpus, id, pages, regions: formatURL(url, corpus, id, pages, regions),
                       StringType())
    getTitle = udf(lambda series: stitle[series] if series in stitle else series, StringType())
    raw\
        .withColumnRenamed("title", "heading")\
        .withColumn("title", getTitle(raw.series))\
        .withColumn("text", removeTags(raw.text))\
        .withColumn("url", constructURL(raw.url, raw.corpus, raw.id, raw.pages, raw.regions))\
        .drop("locs")\
        .drop("pages")\
        .drop("regions")\
        .orderBy(desc("size"), "cluster", "date", "id", "begin")\
        .write.format(outputFormat).options(**outputOptions).save(sys.argv[3])

    sc.stop()
    
