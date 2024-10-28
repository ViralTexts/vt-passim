import argparse, glob, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, sort_array, split, struct, udf, when
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load NCNP XML',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    spark.read.load(config.inputPath, format='text', wholetext=True, recursiveFileLookup=True,
                    pathGlobFilter='*.xml*'
        ).select(f.input_file_name().alias('file'), 'value'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
