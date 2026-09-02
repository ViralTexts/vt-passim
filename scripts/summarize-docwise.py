import argparse, glob, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, split, struct, udf, when
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Summarize docwise alignments',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('metaPath', metavar='<meta path>', help='meta path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    meta = spark.read.json(config.metaPath)

    stats = spark.read.json(config.inputPath
                ).select(f.element_at(split('id', '/'), -3).alias('book'),
                         f.element_at(split('id', '/'), -1).alias('id'),
                         explode('lines').alias('line')
                ).select('book', 'id', explode(col('line')['wits']).alias('wit')
                ).withColumn('ref', col('wit')['ref']
                ).withColumn('src', when(col('ref') == 1, split(col('wit')['id'], '/')[9]).otherwise(f.element_at(split(col('wit')['id'], '/'), -3))
                ).withColumn('matchRate', col('wit')['matches']/f.length(col('wit')['text'])
                ).filter(col('book') != col('src')
                ).filter((col('matchRate') > 0.5) & (col('wit')['matches'] > 2))

    stats.groupBy('book', 'src', 'ref'
        ).agg(f.count('matchRate').alias('lines')
        ).filter(col('lines') >= 10
        ).groupBy('book', 'ref'
        ).agg(f.slice(sort_array(collect_list(struct('lines', 'src')), False), 1, 20).alias('matches')
        ).select('book', 'ref', explode('matches').alias('m')
        ).join(meta, ['book'], 'left_outer'
        ).select('book', 'ref', 'm.*', 'url'
        ).withColumnRenamed('src', 'source'
        ).join(meta.toDF('source', 'source_url'), ['source'], 'left_outer'
        ).select('book', col('ref').alias('reference'), 'lines', 'source', 'url',
                 when(col('ref') == 1,
                      f.regexp_replace('source', r'^(([^\.]+)\.[^\.]+)',
                                       'https://github.com/OpenITI/RELEASE/blob/master/data/$2/$1/$1')).otherwise(col('source_url')).alias('source_url')
        ).repartition(1
        ).sort('book', f.desc('reference'), f.desc('lines'), 'source'
        ).write.csv(config.outputPath + '/booklines', header=True)

    # stats.groupBy('id', 'src', 'ref'
    #     ).agg(f.count('matchRate').alias('lines')
    #     ).filter(col('lines') >= 10
    #     ).groupBy('id', 'ref'
    #     ).agg(f.slice(sort_array(collect_list(struct('lines', 'src')), False), 1, 10).alias('matches')
    #     ).select('id', 'ref', explode('matches').alias('m')
    #     ).select('id', 'ref', 'm.*'
    #     ).repartition(1
    #     ).sort('id', f.desc('ref'), f.desc('lines'), 'src'
    #     ).write.csv(config.outputPath + '/page10lines', header=True)
    
    spark.stop()
