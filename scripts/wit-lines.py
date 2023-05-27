import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, length, sort_array, struct, udf
import pyspark.sql.functions as f

# length of the maximum alignment gap
def maxGap(s):
    res = 0
    cur = 0
    for c in s:
        if c == '-':
            cur += 1
        elif cur > 0:
            if cur > res:
                res = cur
            cur = 0
    if cur > res:
        res = cur
    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Witness lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--min-line', type=int, default=5,
                         help='Minimum length of line', metavar='N')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Witness lines').getOrCreate()

    max_gap = udf(lambda s: maxGap(s), 'int')

    spark.read.json(config.inputPath
        ).select('id', 'lineIDs', col('pages')[0].alias('page'), explode('lines').alias('line')
        ).filter(col('line.wits').isNotNull()
        ).select(col('page.id').alias('img'), 'id', 'lineIDs', col('page.regions'),
                 col('line.begin'), length('line.text').alias('length'),
                 col('line.text').alias('dstText'),
                 f.translate(col('line.wits')[0]['text'], '\n', ' ').alias('srcText'),
                 col('line.wits')[0]['id'].alias('src'),
                 col('line.wits')[0]['matches'].alias('matches'),
                 f.translate(col('line.wits')[0]['alg'], '\n', ' ').alias('srcAlg'),
                 col('line.wits')[0]['alg2'].alias('dstAlg')
        ).filter(col('length') >= config.min_line
        ).withColumn('matchRate',
                     col('matches') / f.greatest(length('dstText'), length('srcText'))
        ).withColumn('maxGap', f.greatest(max_gap('srcAlg'), max_gap('dstAlg'))
        ).withColumn('leadGap', length(f.regexp_extract('dstAlg', r'^\s*(\-+)', 1))
        ).withColumn('tailGap', length(f.regexp_extract('dstAlg', r'(\-+)\s*$', 1))
        ).withColumn('lineID',
                     f.filter('lineIDs', lambda r: (r['start'] >= col('begin')) &
                              (r['start'] + r['length'] <= col('begin') + col('length')))[0]['id']
        ).withColumn('regions',
                     f.filter('regions', lambda r: (r['start'] >= col('begin')) &
                              (r['start'] + r['length'] <= col('begin') + col('length')))
        ).withColumn('x', f.array_min('regions.coords.x')
        ).withColumn('y', f.array_min('regions.coords.y')
        ).withColumn('w',
                     f.array_max(f.transform('regions.coords',
                                             lambda r: r['x'] + r['w'])) - col('x')
        ).withColumn('h',
                     f.array_max(f.transform('regions.coords',
                                             lambda r: r['y'] + r['h'])) - col('y')
        ).drop('regions', 'lineIDs'
        ).sort(f.desc('matchRate')
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
