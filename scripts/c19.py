from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: c19.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="SelectC19")
    sqlContext = SQLContext(sc)
    raw = sqlContext.read.load(sys.argv[1])

    raw.filter(raw.date < '1900').repartition(500).write.save(sys.argv[2])

    sc.stop()
