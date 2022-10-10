import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, xxhash64
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='METS merge',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('METS merge').getOrCreate()

    spark.read.json(config.inputPath, recursiveFileLookup='true', pathGlobFilter='*.json.gz'
        ).filter(col('series').isNotNull()
        ).dropDuplicates(['file']).write.json(config.outputPath, mode='overwrite')

    spark.stop()
    
