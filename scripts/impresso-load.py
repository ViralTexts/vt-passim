from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, udf

def insertBreaks(text, lb):
    for b in reversed(lb):
        if b < len(text):
            text = text[:b] + "[[" + text[b] + "]]" + text[(b+1):]
    return text

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: impresso-load.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Load Impresso JSON').getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')

    insert_breaks = udf(lambda text, lb: insertBreaks(text, lb))
    
    spark.read.json(sys.argv[1]) \
         .select(col('id'), col('d').alias('date'), col('lg').alias('lang'),
                 regexp_extract('id', r'^([^\-]+)\-', 1).alias('series'),
                 regexp_replace('id', r'\-[^\-]+$', '').alias('issue'),
                 col('t').alias('heading'), col('tp').alias('category'),
                 insert_breaks('ft', 'lb').alias('text'),
                 col('pp').alias('pages')) \
         .write.json(sys.argv[2])
    spark.stop()

