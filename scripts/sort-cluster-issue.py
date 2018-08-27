#!/usr/bin/python

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("Usage: sort-cluster-issue.py <input> <output>", file=sys.stderr)
    exit(-1)
  sc = SparkContext(appName='resort data')
  sqlContext = SQLContext(sc)

  df = sqlContext.read.load(sys.argv[1])
  df.registerTempTable("newspaper")
  df2 = sqlContext.sql('select series, date, count(*) as cnt from newspaper group by series, date')
  df3 = df.join(df2, ['series', 'date'])
  df3.sort(desc("cnt"), "series", "date", "id", "begin", "end")\
     .write.option('compression', 'gzip').json(sys.argv[2])

  sc.stop()
  
