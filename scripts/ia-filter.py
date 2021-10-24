from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: ia-filter.py <ids> <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('IA Filter').getOrCreate()

    ids = spark.read.text(sys.argv[1]).toDF('book')

    raw = spark.read.load(sys.argv[2])

    raw.filter(~(col('book').endswith('goog') & (col('seq') <= 1)) & ~(col('book').startswith('jstor-') & (col('seq') <= 0))) \
       .join(broadcast(ids), 'book', 'leftsemi') \
       .write.save(sys.argv[3])

    spark.stop()
