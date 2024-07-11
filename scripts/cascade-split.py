import argparse
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import (col, desc, explode, size, udf, struct, length,
                                   coalesce, collect_list, collect_set, concat_ws, sort_array,
                                   expr, map_from_entries, flatten, xxhash64, lit,
                                   array, arrays_zip)
import pyspark.sql.functions as f

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Cascade Split',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--clusters', type=str, default='size >= 10 AND pboiler < 0.05',
                        help='SQL query for clusters')
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Cascade Split').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    clusters = spark.read.load(config.inputPath)

    if config.clusters:
        clusters = clusters.filter(config.clusters)

    window = Window.partitionBy('cluster').orderBy('date')

    gaps = clusters.select('cluster', 'date'
                    ).distinct(
                    ).withColumn('gap', f.datediff('date', f.lag('date').over(window)))

    maxgap = gaps.groupBy('cluster'
                ).agg(f.max('gap').alias('maxgap'),
                      f.datediff(f.max('date'), f.min('date')).alias('cspan'))

    ccount = gaps.filter(col('gap') > 365
                ).groupBy('cluster'
                ).agg((f.count('date') + 1).alias('cascades'))

    cinfo = maxgap.join(ccount, ['cluster'], 'left_outer'
                ).na.fill(1, subset='cascades'
                ).join(gaps, 'cluster')

    clusters.join(cinfo, ['cluster', 'date']
            ).sort(desc('size'), 'cluster', 'date', 'id', 'begin'
            ).write.json(config.outputPath)
    
    spark.stop()
