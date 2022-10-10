import argparse
from re import sub
import html

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, regexp_replace
import pyspark.sql.functions as f

def formatPassage(r):
    text = ""
    source = 'From ' + (('<cite>%s</cite>' % r.source) or 'unknown source')
    title = r.title or source
    if 'ref' in r and r.ref > 0:
        ref = 1
    else:
        ref = 0
    if r.url:
        text += "<h2><a href=\"%s\">%s</a></h2>" % (r.url, title)
    else:
        text += "<h2>%s</h2>" % title
    if ('creator' in r) and r.creator: text += '<h4>by %s</h4>' % r.creator
    if title != source: text += '<h4>%s</h4>' % source
    dateline = '<datetime>%s</datetime>' % r.date
    if r.placeOfPublication:
        dateline += ' &middot; %s' % r.placeOfPublication
    text += "<h4>%s</h4>" % dateline
    if ref > 0 or r.open == 'true':
        text += '<table style="width: 100%;"><tr><td style="width: 50%">' + sub('(?<!\\\\)\\n', '<br/>\\n', html.escape(r.text)) + '</td>'
        if r.page_image:
            factor = '/!600,600/'
            if r.corpus == 'ia':
                if r.p1h >= r.p1w:
                    factor = '/,600/'
                else:
                    factor = '/600,/'
            scaled = r.page_image.replace('/full/', factor)
            text += f'<td style="width: 50%; max-height: 75%; margin: auto; display: block;"><img src="{scaled}" /></td>'
        text += '</tr></table>'
    else:
        text += '[This text is not available under an open license.]'
    
    return Row(cluster=r.cluster, ref=ref, date=r.date, id=r.id, begin=r.begin, text=text)
    
def formatCluster(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: (-z.ref, z.date, z.id, z.begin))
    name = rows[0].id
    title = "%d reprints from %s to %s [cl%d]" % (len(rows), rows[0].date, rows[len(rows)-1].date, cluster)
    text = f'<html><head><title>{title}</title>\n'
    text += '<meta charset="UTF-8"/>\n</head>\n<body>'
    text += f'<h1>{title}</h1>\n'
    for i in range(len(rows)):
        cur = rows[i].text
        text += "<div n=\"%d\">%s</div><hr />\n" % (i, cur)
    text += '</body></html>'
    return Row(cluster=cluster, suffix=str(cluster)[-2:], name=name, text=text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='GtR Data Dump',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', '--filter', type=str, default='size >= 10 AND pboiler < 0.2',
                        help='SQL query for reprints')
    parser.add_argument('-s', '--suffix', action='store_true', help='Partition by suffix')
    parser.add_argument('prettyPath', metavar='<path>', help='pretty cluster path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('GtR Data Dump').getOrCreate()

    spark.read.load(config.prettyPath
        ).filter(config.filter
        ).drop('locs', 'pages', 'regions'
        ).withColumn('page_access', col('url')
        ).rdd.map(formatPassage
        ).groupBy(lambda r: r.cluster
        ).map(formatCluster
        ).toDF().repartition(200).write.option('compression', 'gzip').json(config.outputPath)
    
    spark.stop()
    
