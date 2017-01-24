from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, datediff

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: c19.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Select c19').getOrCreate()
    raw = spark.read.option('mergeSchema','true').load(sys.argv[1])
    df = raw.filter(col('date') < '1900')

    opens = df.filter(col('open') == 'true')\
              .select('series', 'date', lit(1).alias('inopen')).distinct()

    df.join(opens, ['series', 'date'], 'left_outer')\
      .filter((col('open') == 'true') | col('inopen').isNull())\
      .drop('inopen')\
      .dropDuplicates(['id'])\
      .withColumn('ednum', col('ed').cast('int')).na.fill(1, ['ednum']) \
      .withColumn('hour', datediff(col('date'), lit('1970-01-01')) * 24 + col('ednum') - 1) \
      .drop('ednum') \
      .write.save(sys.argv[2])

    spark.stop()
