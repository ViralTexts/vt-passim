from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, col

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: c19.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="SelectC19")
    sqlContext = SQLContext(sc)
    raw = sqlContext.read.option('mergeSchema','true').load(sys.argv[1])
    df = raw.filter(col('date') < '1900')

    opens = df.filter(col('open') == 'true')\
              .select('series', 'date', lit(1).alias('inopen')).distinct()

    df.join(opens, ['series', 'date'], 'left_outer')\
      .filter((col('open') == 'true') | col('inopen').isNull())\
      .drop('inopen')\
      .dropDuplicates(['id'])\
      .write.save(sys.argv[2])

    sc.stop()
