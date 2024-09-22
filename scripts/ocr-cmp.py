import argparse, json, os, re, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, greatest, length, levenshtein, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO

def UWPR(text, text2):
    if text2 == None:
        text2 = ''
    if text2 == '':
        return (0, 0, 0)
    types1 = set(re.split(r'\W+', text.lower()))
    types2 = set(re.split(r'\W+', text2.lower()))
    ilen = len(types1.intersection(types2))
    prec = ilen/len(types2)
    rec = ilen/len(types1)
    f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0
    return (prec, rec, f1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='OCR comparison',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('goldPath', metavar='<gold path>', help='gold path')
    parser.add_argument('hypPath', metavar='<hyp path>', help='hyp path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    basename = udf(lambda s: re.sub(r'\.([a-z]{1,4})$', '', os.path.basename(s)))
    uwpr = udf(lambda text, text2: UWPR(text, text2),
               'struct<uwp: double, uwr: double, uwf1: double>')

    gold = spark.read.json(config.goldPath).withColumn('base', basename('id'))
    hyp = spark.read.json(config.hypPath)
    fields = [f + '2' for f in hyp.columns]

    hyp.toDF(*fields).withColumn('base', basename('id2')
      ).join(gold, 'base'
      ).withColumn('n', length('text')
      ).withColumn('n2', length('text2')
      ).withColumn('maxlen', greatest('n', 'n2')
      ).withColumn('cer', levenshtein('text', 'text2')/col('maxlen')
      ).withColumn('lbcer', f.abs(col('n') - col('n2'))/col('maxlen')
      ).withColumn('uwpr', uwpr('text', 'text2')
      ).select('*', 'uwpr.*').drop('uwpr'
      ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
