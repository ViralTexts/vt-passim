import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, sort_array, struct, udf
import pyspark.sql.functions as f

## Organize by seq order within the following top-level groups:
group = {
    'top': 0,
    'pageNum': 1,
    'colNum': 2,
    'left': 3,
    'body': 4,
    'right': 5,
    'foot': 6,
    'bottom': 7
}
## everything else (body, table, figure, etc.)

def composePage(zones):
    regions = [ [] for _ in range(len(group)) ]
    for z in zones:
        k = group[z.place if z.place in group else 'body']
        regions[k].append((z.place + '/' + z.ztype, z.text + '\n'))
    text = ''
    res = list()
    for r in regions:
        for z in r:
            res.append((z[0], len(text), len(z[1])))
            text += z[1]
    return (text, res)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Region cat',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Region cat').getOrCreate()

    compose = udf(lambda zones: composePage(zones),
                  'struct<text: string, regions: array<struct<region: string, start: int, length: int>>>')

    spark.read.load(config.inputPath
        ).groupBy(col('page').alias('id'), col('book')
        ).agg(compose(sort_array(collect_list(struct('seq', 'ztype', 'place',
                                                     'text', 'rendition')))).alias('regions')
        ).select('id', 'book', col('regions.*')
        ).write.json(config.outputPath)

    spark.stop()
    
