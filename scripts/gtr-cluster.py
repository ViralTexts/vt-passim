from __future__ import print_function

import sys
from re import sub
import html

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
    dateline = '<datetime>%s</datetime>' % r.date
    if r.placeOfPublication:
        dateline += ' &middot; %s' % r.placeOfPublication
    text += "<h4>%s</h4>" % dateline
    if r.ref > 0 or r.open == 'true':
        text += '<table style="width: 100%;"><tr><td style="width: 50%">' + sub('(?<!\\\\)\\n', '<br/>\\n', html.escape(r.text)) + '</td>'
        if r.page_image:
            scaled = r.page_image.replace('/full/', '/!600,600/')
            text += f'<td style="width: 50%; max-height: 75%; margin: auto; display: block;"><img src="{scaled}" /></td>'
        text += '</tr></table>'
    else:
        text += '[This text is not available under open license.]'
    
    return Row(cluster=r.cluster, ref=r.ref, date=r.date, id=r.id, begin=r.begin, text=text)
    
def formatCluster(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: (-z.ref, z.date, z.id, z.begin))
    name = rows[0].id
    title = "%d reprints from %s to %s [cl%d]" % (len(rows), rows[0].date, rows[len(rows)-1].date, cluster)
    text = f'<html><head><title>{title}</title></head>\n<body>'
    text += f'<h1>{title}</h1>\n'
    for i in range(len(rows)):
        cur = rows[i].text
        text += "<div n=\"%d\">%s</div><hr />\n" % (i, cur)
    text += '</body></html>'
    return Row(name=name, text=text)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: gtr-cluster.py <input> <cluster-out>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('GtR Formatting').getOrCreate()

    df = spark.read.load(sys.argv[1])
    
    df \
        .drop("locs")\
        .drop("pages")\
        .drop("regions")\
        .withColumn('page_access', col('url')) \
        .withColumn('text', regexp_replace(col('text'), '</?[A-Za-z][^>]*>', ''))\
        .rdd \
        .map(formatPassage)\
        .groupBy(lambda r: r.cluster) \
        .map(formatCluster) \
        .toDF().write.json(sys.argv[2])
    
    spark.stop()
    
