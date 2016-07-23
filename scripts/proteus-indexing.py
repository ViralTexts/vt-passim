from __future__ import print_function

import sys
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import coalesce, col, udf, concat_ws, count, regexp_replace

def pageCat(grp):
    (book, pages) = grp
    pp = sorted(pages, key=lambda r: r.seq)
    res = pp[0].asDict()
    res['name'] = book
    res['text'] = "\n".join(['<div class="page-break" page="%d">%s</div>' % (r.seq, r.text) for r in pp]) + ('<archiveid tokenizetagcontent="false">%s</archiveid>' % book)
    return Row(**res)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: pretty-cluster.py <input> <page-out> <book-out>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Proteus Pages")
    sqlContext = SQLContext(sc)

    raw = sqlContext.read.load(sys.argv[1])
    cols = set(raw.columns)
    idcols = [col(x) for x in ['identifier', 'issue', 'book'] if x in cols]

    df = raw.withColumn('identifier', regexp_replace(coalesce(*idcols), '[^A-Za-z0-9]+', ''))

    counts = df.groupBy('identifier').count().select(col('identifier'), col('count').alias('imagecount'))

    appendID = udf(lambda book, text: '%s <archiveid tokenizetagcontent="false">%s</archiveid>' % (text, book))

    renamed = df.join(counts, 'identifier')\
                .drop('regions')\
                .withColumn('pageNumber', col('seq'))\
                .withColumn('name', concat_ws('_', col('identifier'), col('seq')))\
                .withColumn('text', regexp_replace(col('text'), '\\n', '<br>\\\n'))

    renamed.withColumn('text', appendID(col('identifier'), col('text')))\
           .write.format('json').save(sys.argv[2])

    renamed.rdd.groupBy(lambda r: r.identifier).map(pageCat).toDF()\
        .write.format('json').save(sys.argv[3])

    sc.stop()
