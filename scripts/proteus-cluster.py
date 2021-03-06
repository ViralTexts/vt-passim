from __future__ import print_function

import sys
from re import sub

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, regexp_replace

def formatPassage(r):
    text = ""
    source = 'From ' + (('<cite>%s</cite>' % r.source) or 'unknown source')
    title = r.title or source
    if r.url:
        text += "<h2><a href=\"%s\">%s</a></h2>" % (r.url, title)
    else:
        text += "<h2>%s</h2>" % title
    if r.creator: text += '<h4>by %s</h4>' % r.creator
    if title != source: text += '<h4>%s</h4>' % source
    cluster = "cl" + str(r.cluster)
    dateline = '<date tokenizetagcontent="false">%s</date>' % r.date
    if r.placeOfPublication:
        dateline += ' &middot; <place>%s</place>' % r.placeOfPublication
    text += "<h4>%s</h4>" % dateline
    text += sub('(?<!\\\\)\\n', '<br/>\\n', r.text)
    text += ' <archiveid tokenizetagcontent="false">%s</archiveid>' % cluster
    text += ' <series tokenizetagcontent="false">%s</series>' % r.series
    text += ' <id tokenizetagcontent="false">%s</id>' % r.id
    if r.subject:
        text += ' <subject>%s</subject>' % r.subject
    
    return Row(archiveid=cluster, id=r.id, imagecount=r.size, title=r.title, date=r.date, placeOfPublication=r.placeOfPublication, ref=r.ref,
               text=text, page_access=r.page_access, page_image=r.page_image, page_thumb=r.page_thumb)
    

def formatPassages(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: (-z.ref, z.date, z.id))
    res = list()
    for i in range(len(rows)):
        r = rows[i].asDict()
        name = "%s_%d" % (cluster, i)
        r['name'] = name
        r['seq'] = i
        r['identifier'] = cluster
        r['pageNumber'] = i
        res.append(Row(**r))
    return res

def formatCluster(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: (-z.ref, z.date, z.id))
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
    
