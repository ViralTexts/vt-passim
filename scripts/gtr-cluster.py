import argparse
from re import sub
import html

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, regexp_replace, struct
import pyspark.sql.functions as f

def formatPassage(r):
    text = ''
    source = 'From ' + (f'_{r.source}_' if r.source else 'unknown source')
    title = r.title or (source if r.source else None) or r.id
    if 'ref' in r and r.ref > 0:
        ref = 1
    else:
        ref = 0
    if r.url:
        text += "\n## [%s](%s)\n" % (title, r.url)
    else:
        text += "\n## %s\n" % title
    if ('creator' in r) and r.creator: text += '\n#### by %s\n' % r.creator
    if title != source: text += '\n#### %s\n' % source
    dateline = '%s' % r.date
    place = None
    if r.placeOfPublication:
        place = r.placeOfPublication
    elif r.city:
        # Anglocentrism!
        if r.country in ['United States of America', 'United Kingdom', 'Australia']:
            place = f'{r.city}, {r.topdiv}'
        else:
            place = f'{r.city}, {r.country}'
    if place:
        if r.coverage:
            place = f'[{place}]({r.coverage})'
        dateline += ' &middot; %s' % place
    text += "\n#### %s\n\n" % dateline
    if ref > 0 or r.open == 'true':
        text += '<table style="width: 100%;"><tr><td style="width: 50%">\n\n' + sub('(?<!\\\\)\\n', '  \\n', html.escape(r.text)) + '\n</td>'
        if r.page_image:
            factor = '/!600,600/'
            if r.corpus == 'ia':
                if r.p1h >= r.p1w:
                    factor = '/,600/'
                else:
                    factor = '/600,/'
            scaled = r.page_image.replace('/full/', factor)
            text += '<td style="width: 50%%; max-height: 75%%; margin: auto; display: block;">\n<img alt="Page image" src="%s"/>\n</td>\n' % sub('\$', '&#0036;', scaled)
        text += '</tr></table>\n'
    else:
        text += '[This text is not available under an open license.]\n'
    
    return Row(cluster=r.cluster, ref=ref, date=r.date, id=r.id, title=title, begin=r.begin, text=text)
    
def formatCluster(x):
    (cluster, riter) = x
    rows = list(riter)
    rows.sort(key=lambda z: (-z.ref, z.date, z.id, z.begin))
    name = rows[0].id
    title = rows[0].title
    dates = [r.date for r in rows if r.date != None]
    desc = "%d reprints from %s to %s" % (len(rows), min(dates), max(dates))
    text = f'\n# {title}\n\n### {desc}\n'
    for i in range(len(rows)):
        cur = rows[i].text
        text += f'{cur}\n---\n'
    return Row(cluster=cluster, suffix=str(cluster)[-2:], name=name, size=len(rows),
               title=title, description=desc, text=text)

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
        ).toDF(
        ).groupBy('name'
        ).agg(f.max(struct('size', 'cluster', 'suffix', 'title', 'description', 'text')).alias('f')
        ).select('name', 'f.*'
        ).sort(f.regexp_replace('title', r'^(The|An?)[ ]+', '').alias('stitle'), 'name'
        ).write.json(config.outputPath, compression='gzip', mode='overwrite')
    
    spark.stop()
    
