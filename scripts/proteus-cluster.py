from __future__ import print_function

import sys
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col, udf, array_contains, explode, desc
from pyspark.sql.types import StringType

def formatPassage(r):
    text = ""
    if r.url:
        text += "<h2><a href=\"%s\">%s</a></h2>" % (r.url, r.title)
    else:
        text += "<h2>%s</h2>" % r.title
    dateline = r.date
    if r.placeOfPublication:
        dateline += " &middot; %s" % r.placeOfPublication
    text += "<h4>%s</h4>" % dateline
    text += sub('(?<!\\\\)\\n', '<br/>\\n', r.text)
    text += " <archiveid tokenizetagcontent=\"false\">%s</archiveid>" % str(r.cluster)
    return Row(archiveid=str(r.cluster), title=r.title, date=r.date, placeOfPublication=r.placeOfPublication, text=text)
    

def formatPassages(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: z.date)
    res = list()
    for i in range(len(rows)):
        r = rows[i].asDict()
        id = "%s_%d" % (cluster, i)
        r['name'] = id
        res.append(Row(**r))
    return res

def formatCluster(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: z.date)
    text = ""
    for i in range(len(rows)):
        text += "<div class=\"page-break\" page=\"%d\">%s</div>\n" % (i, rows[i].text)
    return Row(name=cluster, date=rows[0].date, title=("Cluster %s (%d)" % (cluster, len(rows))), text=text)


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
        print("Usage: proteus-cluster.py <metadata> <input> <output> [<query>]", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Proteus Formatting")
    sqlContext = SQLContext(sc)

    outpath = sys.argv[3]

    ## Should do more field renaming in meta to avoid clashing with fields in raw.
    meta = sqlContext.read.json(sys.argv[1])\
           .withColumnRenamed('lang', 'series_lang')\
           .withColumnRenamed('id', 'series').dropDuplicates(['series'])
    
    df = sqlContext.read.load(sys.argv[2])
    
    if len(sys.argv) >= 5:
        print(sys.argv[4])
        raw = df.join(df.filter(sys.argv[4]).select("cluster").distinct(), 'cluster')
    else:
        raw = df

    h = HTMLParser.HTMLParser()
    removeTags = udf(lambda s: sub("</?[A-Za-z][^>]*>", "", s), StringType())
    constructURL = udf(lambda url, corpus, id, pages, regions: formatURL(url, corpus, id, pages, regions),
                       StringType())

    clusters \
        = raw\
        .withColumnRenamed("title", "heading")\
        .withColumn("url", constructURL(raw.url, raw.corpus, raw.id, raw.pages, raw.regions))\
        .drop("locs")\
        .drop("pages")\
        .drop("regions")\
        .withColumn("text", removeTags(raw.text))\
        .join(meta, 'series', 'left_outer')\
        .map(formatPassage)\
        .groupBy(lambda r: r.archiveid)

    clusters.flatMap(formatPassages).toDF().write.json(outpath + "/pass.json")

    clusters.map(formatCluster).toDF().write.json(outpath + "/cluster.json")

    sc.stop()
    
