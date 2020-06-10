from __future__ import print_function

import sys
import textwrap

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, concat, concat_ws, regexp_replace, udf

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: trove-load.py <input json> <api json> <output parquet>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Trove Load').getOrCreate()

    wrapper = textwrap.TextWrapper(width=40, break_long_words=False)

    text_fill = udf(lambda s: wrapper.fill(s))

    xunesc = udf(lambda s: s \
                    .replace('&lt;', '<') \
                    .replace('&amp;', '&') \
                    .replace('&gt;', '>') \
                    .replace('&quot;', '"') \
                    .replace('&apos;', "'"))

    raw = spark.read.json(sys.argv[1])
    api = spark.read.json(sys.argv[2])
    
    df = raw.na.drop(subset=['id', 'fulltext']).dropDuplicates(['id']) \
            .select(concat(lit('trove/'), col('id')).alias('id'),
                    concat_ws('/', lit('trove'), col('titleId'), col('date')).alias('issue'),
                    concat(lit('trove/'), col('titleId')).alias('series'),
                    col('date'), col('firstPageId'),
                    col('firstPageSeq').cast('int').alias('seq'),
                    col('heading').alias('title'), col('category'),
                    text_fill(col('fulltext')).alias('text'))

    apitext = api.select(concat(lit('trove/'), col('article.id')).alias('id'),
                         xunesc(regexp_replace(regexp_replace(regexp_replace(
                             col('article.articleText'), r'<(span|p)>[ ]*', ''),
                                                              r'</(span|p)>[ ]*', '\n'),
                                               r' [ ]*', ' ')).alias('articleText'))

    df.join(apitext, ['id'], 'left_outer') \
      .withColumn('text', coalesce('articleText', 'text')) \
      .drop('articleText') \
      .write.save(sys.argv[3])

    spark.stop()
