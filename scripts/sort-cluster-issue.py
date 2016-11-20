#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import asc, desc

if __name__ == "__main__":
  sc = SparkContext(appName='resort data')
  sqlContext = SQLContext(sc)

  df = sqlContext.read.load('hdfs://discovery3:9000/tmp/dasmith/c19-20160919-a50-o08/pretty.parquet')
  #df = sqlContext.read.load('hdfs://discovery3:9000/tmp/dasmith/c19-20160402-a50-o08/out.parquet')
  df.registerTempTable("newspaper")
  df2 = sqlContext.sql("select series, date, count(*) as cnt from newspaper group by series, date order by cnt desc")
  df3 = df.join(df2, ['series', 'date'])
  df3.sort(desc("cnt"), asc("begin"), asc("end"))\
     .write.json('/gss_gpfs_scratch/xu.shao/network/resorted-pretty.json')
