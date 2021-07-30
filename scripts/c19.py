from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, struct, max, min

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: c19.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Select c19').getOrCreate()
    raw = spark.read.option('mergeSchema','true').load(sys.argv[1])
    df = raw.filter(col('date') < '1900')

    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    issues = df.groupBy('series', 'date'
                ).agg(min(struct(~col('open').cast('boolean'),
                                 col('corpus')))['corpus'].alias('corpus'))

    df.join(broadcast(issues), ['series', 'date', 'corpus'], 'left_semi'
        ).write.partitionBy('open', 'corpus').save(sys.argv[2])

    spark.stop()
