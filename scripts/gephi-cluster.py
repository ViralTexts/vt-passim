from __future__ import print_function

import sys, codecs
from xml.sax.saxutils import quoteattr
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, array_contains, explode, desc
from pyspark.sql.types import StringType

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: gephi-cluster.py <metadata> <input> [dynamic?]", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Gephi Cluster")
    sqlContext = SQLContext(sc)

    if len(sys.argv) >= 4 and sys.argv[3] == 'dynamic':
        dynamic = True
    else:
        dynamic = False

    meta = sqlContext.read.json(sys.argv[1])

    raw = sqlContext.read.load(sys.argv[2])

    ## There aren't many series.  It'll be cleaner to use a closure instead of a join
    sid = dict(meta.select(explode(meta.publication_names.sn).alias("series"),
                           meta.id).distinct().collect())

    stitle = dict(meta.select(explode(meta.publication_names.sn).alias("series"),
                              meta.master_name).distinct().collect())
    mtitle = dict(meta.select(meta.id, meta.master_name).distinct().collect())

    getSeries = udf(lambda series: sid[series] if series in sid else series, StringType())
    smax = udf(lambda a, b: max(a, b), StringType())

    df = raw.select(raw.cluster,
                    getSeries(raw.series).alias('series'),
                    raw.date.substr(1,4).alias('year')).na.drop()

    nodes = df.select(df.series).distinct().collect()

    print('<gexf>')
    if dynamic:
        print('  <graph defaultedgetype="undirected" mode="dynamic" timeformat="date">')
    else:
        print('  <graph defaultedgetype="undirected">')
    print('    <nodes>')
    for x in nodes:
        m = x.asDict()
        s = m['series']
        t = mtitle[s] if s in mtitle else (stitle[s] if s in stitle else s)
        print('      <node id="%s" label=%s />' % (s, quoteattr(t).encode('ascii', 'xmlcharrefreplace')))
    print('    </nodes>')
    print('    <edges>')

    df2 = df.select(df.cluster.alias('cluster2'),
                    df.series.alias('series2'), df.year.alias('year2'))

    joint = df.join(df2,
                    (df.cluster == df2.cluster2) & (df.series < df2.series2),
                    'inner')

    if not dynamic:
        links = joint.select(joint.series, joint.series2)\
                     .groupBy('series', 'series2')\
                     .count().orderBy('series', 'series2').collect()

        for x in links:
            m = x.asDict(True)
            id = m['series'] + '--' + m['series2']
            print('      <edge id="%s" source="%s" target="%s" weight="%d" />' % (id, m['series'], m['series2'], m['count']) )

    ## Aggregate per pair per year

    else:
        links = joint.select(joint.series, joint.series2,
                             smax(joint.year, joint.year2).alias('year'))\
                     .groupBy('series', 'series2', 'year')\
                     .count().orderBy('series', 'series2', 'year').collect()

        for x in links:
            m = x.asDict(True)
            id = m['series'] + '--' + m['series2'] + '--' + m['year']
            print('      <edge id="%s" source="%s" target="%s" weight="%d" start="%s-01-01" end="%s-12-31" label="%s" />' %
                  (id, m['series'], m['series2'], m['count'], m['year'], m['year'], m['year']))
        
    print('  </edges>\n</graph>\n</gexf>')

    sc.stop()
    
