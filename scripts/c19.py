import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import broadcast, coalesce, col, struct, max, min
import pyspark.sql.functions as f

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Select C19',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--min-year', type=int, default=0,
                        help='Minimum year')
    parser.add_argument('--max-year', type=int, default=1900,
                        help='Maximum year')
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('groupPath', metavar='<path>', help='group path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Select c19').getOrCreate()

    groups = spark.read.csv(config.groupPath, header=True)
    raw = spark.read.load(config.inputPath, mergeSchema=True)
    df = raw.withColumn('day', col('date').cast('date')
            ).drop('group', 'daylag'
            ).filter( (f.year('day') >= config.min_year) & (f.year('day') < config.max_year) )

    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    window = Window.partitionBy('series').orderBy('day')
    issues = df.groupBy('series', 'day'
                ).agg(min(struct(~col('open').cast('boolean'),
                                 col('corpus')))['corpus'].alias('corpus')
                ).withColumn('daylag', f.datediff('day', f.lag('day').over(window))
                ).na.fill(0, 'daylag')

    df.join(broadcast(issues), ['series', 'day', 'corpus']
        ).join(broadcast(groups), 'series', 'left_outer'
        ).withColumn('group', coalesce('group', 'series')
        ).write.partitionBy('open', 'corpus').save(config.outputPath)

    spark.stop()
