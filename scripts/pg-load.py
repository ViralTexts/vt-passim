import argparse, os, regex, sys
from unicodedata import normalize
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (array_join, col, collect_list, element_at, regexp_replace,
                                   sort_array, split, struct, udf)
import pyspark.sql.functions as f

def greekText(s):
    res = ''
    for line in s.split('\n'):
        line = sub(r'</?[A-Za-z][^>]*>', '', line)
        if len(line) > 0 and len(regex.findall(r'\p{InGreek}', line))/len(line) > 0.5:
            res += sub(r'\s+$', '', line)
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
    nfd_norm = udf(lambda s: normalize('NFD', s))
    greek_text = udf(lambda s: greekText(s))

    meta = spark.read.csv(config.metaPath, header=True, escape='"'
               ).select('book',
                        f.format_string('urn:cts:greekLit:pg%s.pg-grc1', 'prnc').alias('id'),
                        col('dates').alias('date'))

    spark.read.text(config.inputPath, recursiveFileLookup=True, wholetext=True,
                    pathGlobFilter='*.xml'
        ).withColumn('parts', split(f.input_file_name(), '/')
        ).select(element_at('parts', -2).alias('book'),
                 regexp_replace(element_at('parts', -1), r'\.xml$', '').cast('int').alias('pos'),
                greek_text(nfd_norm('value')).alias('text')
        # ).filter(~col('text').contains('ιιι')
        # ).groupBy('book'
        # ).agg(array_join(sort_array(collect_list(struct('pos', 'text')))['text'],
        #                  '\n').alias('text')
        ).join(meta, 'book'
        # ).select('id', f.format_string('njp.%s', 'book').alias('book'),
        #          regexp_replace('text', r'\n\n+', '\n\n').alias('text')
        ).write.json(config.outputPath, mode='overwrite')
    
    spark.stop()
