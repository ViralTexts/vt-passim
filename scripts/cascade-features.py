from __future__ import print_function

import json
import sys
import numpy as np
from math import log

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import abs as colabs, col, datediff, lit, udf, when, explode, desc
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import CountVectorizer

def maxGap(pair):
    (cluster, days) = pair
    udays = sorted(set(days))
    if len(udays) < 2:
        return (cluster, 0)
    else:
        return (cluster, int(np.max(np.ediff1d(udays))))

def pairFeatures(parent, child, pday, cday):
    lag = abs(cday - pday)      # throw away the sign to allow learning
    lagBin = str(int(log(lag)))
    return ["parent:" + parent, "child:" + child, "pair:" + parent + ":" + child,
            "lag:" + lagBin]

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: cascade-features.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName='Cascade Features')
    sqlContext = SQLContext(sc)
    
    raw = sqlContext.read.load(sys.argv[1])
    df = raw.dropDuplicates(['cluster', 'series', 'date'])\
            .withColumn('day', datediff(col('date'), lit('1970-01-01')))\
            .na.drop(subset=['day'])

    df2 = df.select([col(x).alias(x + '2') for x in df.columns])

    gap = 730

    pairs = df.join(df2, (df.cluster == df2.cluster2) & (df.day != df2.day2) & (colabs(df.day - df2.day2) < gap))\
              .select('cluster', 'series', 'date', 'day', 'series2', 'date2', 'day2')

    getPairFeatures = udf(lambda series, series2, day, day2: pairFeatures(series, series2, day, day2),
                          ArrayType(StringType()))

    res = pairs.withColumn('label', when(pairs.day2 > pairs.day, 1).otherwise(0))\
               .withColumn('raw', getPairFeatures(pairs.series, pairs.series2, pairs.day, pairs.day2))
    res.cache()

    cv = CountVectorizer(inputCol='raw', outputCol='features', minDF=4.0)
    interner = cv.fit(res)      # alternate possibility: grab features only from label==1 edges

    json.dump(interner.vocabulary, open(sys.argv[2], 'w'))

    interner.transform(res).write.save(sys.argv[3])
    
    sc.stop()
