import argparse, glob, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (array_join, col, collect_list,
                                   concat, explode, format_string, lit, regexp_replace,
                                   sort_array, split, struct, udf, when)
import pyspark.sql.functions as f

def pnoFormat(book, seq, pno):
    res = '<pb '
    if pno != None and pno != '':
        res += f'n="{pno}" '
    res += 'facs="%s_%05d" />' % (book, seq)
    return res

def textFormat(text):
    return sub('<', '&lt;', sub('&', '&amp;', text))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MDZ text dump',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    pno_format = udf(lambda book, seq, pno: pnoFormat(book, seq, pno))
    text_format = udf(lambda text: textFormat(text))

    spark.read.load(config.inputPath
        ).withColumn('data', concat(pno_format('book', 'seq', 'pno'), lit('\n'),
                                    text_format('text'))
        ).groupBy('book'
        ).agg((sort_array(collect_list(struct('seq', 'data')))['data']).alias('data')
        ).select(format_string('<?xml version="1.0" encoding="UTF-8"?>\n<text>\n%s\n</text>', array_join('data', '\n')).alias('text')
        ).write.text(config.outputPath)
