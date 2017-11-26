from __future__ import print_function

import sys
from re import sub

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, regexp_replace

def formatPassage(r):
    text = ""
    if r.url:
        text += "<h2><a href=\"%s\">%s</a></h2>" % (r.url, r.title)
    else:
        text += "<h2>%s</h2>" % r.title
    cluster = "cl" + str(r.cluster)
    dateline = r.date
    if r.placeOfPublication:
        dateline += " &middot; %s" % r.placeOfPublication
    text += "<h4>%s</h4>" % dateline
    text += sub('(?<!\\\\)\\n', '<br/>\\n', r.text)
    text += " <archiveid tokenizetagcontent=\"false\">%s</archiveid>" % cluster
    return Row(archiveid=cluster, imagecount=r.size, title=r.title, date=r.date, placeOfPublication=r.placeOfPublication,
               text=text, page_access=r.page_access)
    

def formatPassages(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: z.date)
    res = list()
    for i in range(len(rows)):
        r = rows[i].asDict()
        id = "%s_%d" % (cluster, i)
        r['name'] = id
        r['id'] = id
        r['seq'] = i
        r['identifier'] = cluster
        r['pageNumber'] = i
        res.append(Row(**r))
    return res

def formatCluster(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: z.date)
    text = ""
    for i in range(len(rows)):
        text += "<div class=\"page-break\" page=\"%d\">%s</div>\n" % (i, rows[i].text)
    return Row(name=cluster, identifier=cluster, imagecount=rows[0].imagecount, date=rows[0].date,
               title=("%d reprints from %s to %s [%s]" % (len(rows), rows[0].date, rows[len(rows)-1].date, cluster)), text=text, pageNumber=0, seq=0, id=cluster+'_0')


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: proteus-cluster.py <input> <page-out> <cluster-out>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Proteus Formatting').getOrCreate()

    df = spark.read.load(sys.argv[1])
    
    clusters = df \
               .drop("locs")\
               .drop("pages")\
               .drop("regions")\
               .withColumn('page_access', col('url')) \
               .withColumn('text', regexp_replace(col('text'), '</?[A-Za-z][^>]*>', ''))\
               .rdd \
               .map(formatPassage)\
               .groupBy(lambda r: r.archiveid)

    clusters.flatMap(formatPassages).toDF().write.option('compression', 'gzip').json(sys.argv[2])

    clusters.map(formatCluster).toDF().write.option('compression', 'gzip').json(sys.argv[3])
    
    spark.stop()
    
