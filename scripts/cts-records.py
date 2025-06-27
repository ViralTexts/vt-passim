import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, length, sort_array, struct, udf
import pyspark.sql.functions as f

def catLocs(recs):
    text = ''
    locs = []
    for r in recs:
        locs.append((r.loc, len(text), len(r.text)))
        text += r.text
    return (text, locs)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import CTS Records',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Import CTS Records').getOrCreate()

    cat_locs = udf(lambda locs: catLocs(locs),
                   'struct<text: string, locs: array<struct<loc: string, start: int, length: int>>>')

    spark.read.json(config.inputPath
        ).filter(col('text').isNotNull()
        ).groupBy(col('book').alias('id')
        ).agg(cat_locs(sort_array(collect_list(struct('seq', 'text', 'loc')))).alias('loc')
        ).select('id', 'loc.*'
        ).sort('id'
        ).write.save(config.outputPath)

    spark.stop()
