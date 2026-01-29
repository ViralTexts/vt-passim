import argparse, os, regex, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array_join, col, collect_list, regexp_replace, sort_array, struct, udf
import pyspark.sql.functions as f

def greekText(s):
    res = ''
    for line in s.split('\n'):
        if len(line) > 0 and len(regex.findall(r'\p{InGreek}', line))/len(line) > 0.5:
            res += line
        res += '\n'
    return sub(r'\n\n+', '\n\n', res) + '\n'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Grab IDI Greek text',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('metaPath', metavar='<meta path>', help='meta path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()
    greek_text = udf(lambda s: greekText(s))

    meta = spark.read.json(config.metaPath)

    spark.read.load(config.inputPath
        ).withColumn('gtext', greek_text('text')
        ).groupBy('book',
                  col('title').alias('book_title'), col('author').alias('book_author'),
                  'ocr_score_src', 'ocr_score_gen'
        ).agg(regexp_replace(array_join(sort_array(collect_list(struct('pos', 'gtext')))['gtext'],
                                        '\n'), r'\n\n+', '\n\n').alias('text')
        ).withColumn('text', regexp_replace(regexp_replace('text', r'-\n+', ''), r'\n+', ' ')
        ).join(meta, 'book'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
