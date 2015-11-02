from __future__ import print_function

import sys
from re import sub
import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, array_contains, explode, desc
from pyspark.sql.types import StringType

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pretty-cluster.py <input> <output> <query>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Parquet Load")
    sqlContext = SQLContext(sc)

    df = sqlContext.read.load(sys.argv[1])
    df.registerTempTable("clusters")

    res = sqlContext.sql(sys.argv[3]).select("cluster").distinct()

    df.join(res, "cluster").write.save(sys.argv[2])

    sc.stop()
    
