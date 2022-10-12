import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, coalesce, col, struct, max, min

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Select C19',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('groupPath', metavar='<path>', help='group path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Select c19').getOrCreate()

    groups = spark.read.csv(config.groupPath, header=True)
    raw = spark.read.load(config.inputPath, mergeSchema=True)
    df = raw.filter(col('date') < '1900')

    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    issues = df.groupBy('series', 'date'
                ).agg(min(struct(~col('open').cast('boolean'),
                                 col('corpus')))['corpus'].alias('corpus'))

    df.join(broadcast(issues), ['series', 'date', 'corpus'], 'left_semi'
        ).join(broadcast(groups), 'series', 'left_outer'
        ).withColumn('group', coalesce('group', 'series')
        ).write.partitionBy('open', 'corpus').save(config.outputPath)

    spark.stop()
