from __future__ import print_function

import sys
import textwrap

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, concat_ws, regexp_replace, udf

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: trove-load.py <input json> <output parquet>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Trove Load').getOrCreate()

    wrapper = textwrap.TextWrapper(width=40, break_long_words=False)

    text_fill = udf(lambda s: wrapper.fill(s))

    raw = spark.read.json(sys.argv[1])
    df = raw.na.drop(subset=['id', 'fulltext']).dropDuplicates(['id'])
    df.select(concat(lit('trove/'), df.id).alias('id'),
              concat_ws('/', lit('trove'), df.titleId, df.date).alias('issue'),
              concat(lit('trove/'), df.titleId).alias('series'),
              df.date, df.firstPageId, df.firstPageSeq.cast('int').alias('seq'),
              df.heading.alias('title'), df.category,
              text_fill(regexp_replace(regexp_replace(df.fulltext, '&', '&amp;'),
                             '<', '&lt;')).alias('text'))\
      .write.save(sys.argv[2])

    spark.stop()
