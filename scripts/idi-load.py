import argparse, unicodedata
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, posexplode, sort_array, struct, udf
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load IDI books',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    spark.read.load(config.inputPath
        ).select(col('barcode_src').alias('book'),
                 col('title_src').alias('title'),
                 col('author_src').alias('author'),
                 'date1_src', 'date2_src', 'date_types_src',
                 'language_src', 'language_gen',
                 'ocr_score_src', 'ocr_score_gen',
                 posexplode('text_by_page_src')
        ).withColumn('id', concat_ws('_', 'book', col('pos').cast('string'))
        ).withColumnRenamed('col', 'text'
        ).write.save(config.outputPath)

    spark.stop()
