import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, length, regexp_replace, translate, udf

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import Gutenberg',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--max-chars', type=int, default=8000000)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('metaPath', metavar='<meta path>', help='meta path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Import Gutenberg').getOrCreate()

    base_name = udf(lambda s: os.path.basename(s))

    meta = spark.read.csv(config.metaPath, header=True)
    minmeta = meta.select('id', 'title', col('author').alias('creator'),
                          (col('language')).alias('lang'))

    spark.read.load(config.inputPath, format='binaryFile', recursiveFileLookup='true'
        ).select(regexp_replace(base_name('path'), '_text.txt$', '').alias('id'),
                 regexp_replace(col('content').cast('string'), "[ \\t]+", " ").alias('text')
        ).filter( length('text') <= config.max_chars
        ).join(minmeta, 'id'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
