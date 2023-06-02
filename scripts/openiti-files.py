import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, input_file_name, regexp_replace, translate

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import OpenITI',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Import OpenITI').getOrCreate()

    wd = os.getcwd()
    wd = wd if wd.startswith('file://') else 'file://' + wd

    spark.read.text(config.inputPath, wholetext=True
        ).select(regexp_replace(input_file_name(), '^' + wd + '/', '').alias('id'),
                 regexp_replace(
                     regexp_replace(
                         regexp_replace(
                             regexp_replace(
                                 regexp_replace(translate('value', '#~%|', ''),
                                                r'PageV\S+|ms\d+|\p{M}', ''),
                                 r'\p{P}', ' '),
                             r' [ ]+', ' '),
                         r'\n[ ]+', '\n'),
                     r'[ ]+\n', '\n').alias('text')
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
