import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, regexp_replace, udf
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='One text record per blank-line-delimited block',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    wd = os.getcwd()
    wd = wd if wd.startswith('file:///') else 'file://' + wd

    groupField = 'book'
    spark.read.text(config.inputPath, recursiveFileLookup=True, wholetext=True
        ).select(regexp_replace(f.input_file_name(), '^' + wd + '/', '').alias(groupField),
                 f.posexplode(f.split('value', r'\n\n\n*'))
        ).select(concat_ws('_', groupField, 'pos').alias('id'), groupField, 'pos',
                 col('col').alias('text')
        ).write.save(config.outputPath)

    spark.stop()
