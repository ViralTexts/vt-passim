from __future__ import print_function

import sys
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import StringType

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: tcp-indexing.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="TCP Indexing")
    sqlContext = SQLContext(sc)

    df = sqlContext.read.load(sys.argv[1])

    appendID = udf(lambda book, text: text + " <archiveid tokenizetagcontent=\"false\">" + book + "</archiveid>", StringType())
    
    renamed = df.withColumnRenamed('series', 'identifier')\
                .withColumnRenamed('pagecount', 'imagecount')\
                .withColumnRenamed('book_access', 'identifier-access')\
                .withColumn('pageNumber', df.seq)\
                .withColumn('name', df.id)\
                .withColumn('text', regexp_replace(df.text, '\\n', '<br>\\\n'))

    renamed.withColumn('text', appendID(renamed.identifier, renamed.text))\
           .write.format('json').save(sys.argv[2])

    sc.stop()
