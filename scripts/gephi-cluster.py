from __future__ import print_function

import sys, codecs
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, array_contains, explode, desc
from pyspark.sql.types import StringType

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: gephi-cluster.py <metadata> <input>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Gephi Cluster")
    sqlContext = SQLContext(sc)

    meta = sqlContext.read.json(sys.argv[1])

    raw = sqlContext.read.load(sys.argv[2])

    ## There aren't many series.  It'll be cleaner to use a closure instead of a join
    sid = dict(meta.select(explode(meta.publication_names.sn).alias("series"),
                           meta.id).distinct().collect())

    stitle = dict(meta.select(explode(meta.publication_names.sn).alias("series"),
                              meta.master_name).distinct().collect())

    getTitle = udf(lambda title, series: stitle[series] if series in stitle else series, StringType())
    getSeries = udf(lambda series: sid[series] if series in sid else series, StringType())
    smax = udf(lambda a, b: max(a, b), StringType())

    df = raw.select(raw.cluster,
                    getTitle(raw.title, raw.series).alias('title'),
                    getSeries(raw.series).alias('series'),
                    raw.date.substr(1,4).alias('year')).na.drop()

    nodes = df.select(df.series, df.title).distinct().collect()

    print('<gexf>')
    print('  <graph defaultedgetype="undirected" mode="dynamic" timeformat="date">')
    print('    <nodes>')
    for x in nodes:
        m = x.asDict()
        print('      <node id="%s" label="%s" />' % (m['series'], m['title'].encode('ascii', 'ignore')))
    print('    </nodes>')

    df2 = df.select(df.cluster.alias('cluster2'),
                    df.series.alias('series2'), df.year.alias('year2'))

    joint = df.join(df2,
                    (df.cluster == df2.cluster2) & (df.series < df2.series2),
                    'inner')
    links = joint.select(joint.series, joint.series2,
                         smax(joint.year, joint.year2).alias('year'))\
                 .groupBy('series', 'series2', 'year')\
                 .count().orderBy('series', 'series2', 'year').collect()

    print('    <edges>')
    curid = ''
    for x in links:
        m = x.asDict(True)
        id = m['series'] + '--' + m['series2']
        if id != curid:
            if curid != '':
                print('    </attvalues>\n    </edge>')
            curid = id
            print('    <edge id="%s" source="%s" target="%s">\n    <attvalues>' % (id, m['series'], m['series2']))
        print('      <attvalue for="reprint_count" start="%s" end="%s" value="%d" />' % (m['year'], m['year'], m['count']))
    if curid != '':
        print('    </attvalues>\n    </edge>')
    print('  </edges>\n</graph>\n</gexf>')

    sc.stop()
    
