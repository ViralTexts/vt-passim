import argparse, os, regex, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, regexp_replace
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Grab IDI Greek text',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('oglPath', metavar='<OGL path>', help='OGL path')
    parser.add_argument('workPath', metavar='<work path>', help='work path')
    parser.add_argument('pgPath', metavar='<PG path>', help='PG path')
    parser.add_argument('bookPath', metavar='<book path>', help='book path')
    parser.add_argument('authorPath', metavar='<author path>', help='author path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    work = spark.read.load(config.workPath
               ).select(col('id').alias('book'),
                        lit('misc').alias('batch'),
                        regexp_replace(regexp_replace('text', r'-\n+', ''),
                                       r'\n+', ' ').alias('text'))

    pg = spark.read.load(config.pgPath
             ).select(col('id').alias('book'), 'date',
                      lit('pg').alias('batch'),
                      regexp_replace(regexp_replace('text', r'-\n+', ''),
                                     r'\n+', ' ').alias('text'))

    books = spark.read.load(config.bookPath
                ).drop('ocr_score_gen'
                ).withColumn('batch', lit('idi'))

    authors = spark.read.csv(config.authorPath, header=True, escape='"'
                  ).select(col('urn').alias('group'), 'author', 'date')

    spark.read.load(config.oglPath
        ).filter(col('book').contains(':greekLit:')
        ).drop('id', 'pos'
        ).withColumn('batch', lit('ogl')
        ).unionByName(work, allowMissingColumns=True
        ).withColumn('group', regexp_replace('book', r'^(urn:cts:greekLit:[^.:]+).*$', '$1')
        ).join(authors, ['group'], 'left_outer'
        ).unionByName(pg, allowMissingColumns=True
        ).withColumn('period', lit('pre')
        ).withColumn('duplicate', lit(0.0)
        ).unionByName(books, allowMissingColumns=True
        ).repartition(50
        ).write.save(config.outputPath, mode='overwrite')
