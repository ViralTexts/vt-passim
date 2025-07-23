import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, regexp_replace
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='One text record per line',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    wd = os.getcwd()
    wd = wd if wd.startswith('file:///') else 'file://' + wd

    groupField = 'book'
    spark.read.text(config.inputPath, recursiveFileLookup=True
        ).withColumn('series', regexp_replace(f.input_file_name(), '^' + wd + '/', '')
        ).withColumn('seq', f.monotonically_increasing_id()
        ).select(concat_ws('_', 'series', col('seq').cast('string')).alias('id'),
                 'series', 'seq', col('value').alias('text')
        ).write.save(config.outputPath)

    spark.stop()
