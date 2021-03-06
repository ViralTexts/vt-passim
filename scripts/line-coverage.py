from __future__ import print_function
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, sort_array, explode, struct, size, sum

def lineRecord(r):
    res = []
    off1 = r.b1
    off2 = r.b2
    for line in r.pairs:
        res.append(Row(id=r.id2, begin=off2, text=line['_2'],
                       wit=Row(id=r.id1, begin=off1, text=line['_1'])))
        off1 = off1 + len(line['_1'])
        off2 = off2 + len(line['_2'])
    return res

def corpusLines(r):
    res = []
    off = 0
    for line in r.text.splitlines(True):
        res.append(Row(id=r.id, begin=off, text=line))
        off = off + len(line)
    return res

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: line-coverage.py <input> <corpus> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Line Coverage').getOrCreate()

    attested = \
      spark.read.load(sys.argv[1])\
           .rdd.flatMap(lineRecord)\
           .toDF()\
           .groupBy('id', 'begin', 'text') \
           .agg(sort_array(collect_list('wit')).alias('witnesses'))

    spark.read.load(sys.argv[2])\
         .rdd.flatMap(corpusLines)\
         .toDF()\
         .join(attested.drop('text'), ['id', 'begin'], 'left_outer')\
         .withColumn('haswit', col('witnesses').isNotNull().cast('int'))\
         .groupBy('id')\
         .agg(sort_array(collect_list(struct('begin', 'text', 'witnesses'))).alias('lines'),
              sum('haswit').alias('witlines'))\
         .filter(col('witlines') > 0)\
         .sort('id')\
         .write.option('compression', 'gzip').json(sys.argv[3])

    spark.stop()

# Implement later:
# val w = df.withColumn("sregions", when('stext.isNull, null).otherwise('sregions)).select('id, 'begin, 'text, 'regions, struct('sid as "id", 'sbegin as "begin", 'stext as "text", 'sregions as "regions") as "wit").groupBy("id", "begin", "text", "regions").agg(sort_array(collect_list("wit")) as "witnesses").withColumn("witnesses", when($"witnesses.id"(0).isNull, null).otherwise('witnesses))
# w: org.apache.spark.sql.DataFrame = [id: string, begin: bigint ... 3 more fields]

# scala> w.withColumn("haswit", 'witnesses.isNotNull.cast("int")).groupBy("id").agg(sort_array(collect_list(struct("begin", "text", "regions", "witnesses"))) as "lines", sum("haswit") as "witlines").filter('witlines > 0).sort('id).write.option("compression", "gzip").json("quixote-self/regwit-pages.json")
                                                                                
