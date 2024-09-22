import argparse, json, os, re, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, greatest, length, levenshtein, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='OCR comparison',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    spark.read.json(config.inputPath
        ).select('base', 'model', 'n', 'n2', 'cer', 'lbcer', 'uwp', 'uwr', 'uwf1'
        ).na.fill(0
        ).repartition(1
        ).write.csv(config.outputPath, header=True, mode='overwrite')

    spark.stop()
