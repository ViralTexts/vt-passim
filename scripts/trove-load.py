from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, concat, concat_ws, regexp_replace

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: trove-load.py <input json> <output parquet>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Trove Load")
    sqlContext = SQLContext(sc)

    raw = sqlContext.read.json(sys.argv[1])
    df = raw.na.drop(subset=['id', 'fulltext']).dropDuplicates(['id'])
    df.select(concat(lit('trove/'), df.id).alias('id'),
              concat_ws('/', lit('trove'), df.titleId, df.date).alias('issue'),
              concat(lit('trove/'), df.titleId).alias('series'),
              df.date, df.firstPageId, df.firstPageSeq.alias('seq'),
              df.heading.alias('title'), df.category,
              regexp_replace(df.fulltext, '<', '&lt;').alias('text')).write.save(sys.argv[2])

    sc.stop()
