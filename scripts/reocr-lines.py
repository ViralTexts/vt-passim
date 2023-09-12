import argparse, os, re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, col, collect_list, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from kraken.lib import models


def ocrLines(bcmodel, text, pages):
    off = 0
    regions = pages[0].regions
    i = 0
    for line in text.splitlines(keepends=True):
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Re-OCR pre-segmented lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('modelPath', metavar='<model path>', help='model path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Re-OCR pre-segmented lines').getOrCreate()

    model = models.load_any(config.modelPath)

    bcmodel = spark.sparkContent.broadcast(model)

    ocr_lines = udf(lambda text, pages: ocrLines(bcmodel, text, pages),
                    'array<struct<text: string, orig: text, start: int, length: int, x: int, y: int, w: int, h: int>>').asNondeterministic()

    spark.read.json(config.inputPath
        ).withColumn('lines', ocr_lines('text', 'pages')
        ).write.json(config.outputPath)

    spark.stop()


