from __future__ import print_function

import json
import sys
import numpy as np
from numpy.linalg import inv
from math import log

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import abs as colabs, col, datediff, lit, udf, when, explode, desc, sum as gsum
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
    if cday == pday:
        lag = 0.1
    else:
        lag = abs(cday - pday)      # throw away the sign to allow learning
    lagBin = str(int(log(lag)))
    return ["parent:" + parent, "child:" + child, "pair:" + parent + ":" + child,
            "lag:" + lagBin]

def numberWitnesses(c):
    wits = c[1]
    return [Row(wid=idx+1, **(r.asDict())) for r, idx
            in zip(sorted(wits, key=lambda w: w.date + ':' + w.series), range(len(wits)))]

def padUpleft(m):
    size = m.shape[0]
    return np.concatenate((np.zeros((size+1, 1)),
                           np.concatenate((np.zeros((1, size)), m), axis=0)),
                          axis=1)

def featureGradients(c, w):
    n = max(map(lambda r: r.wid2, c[1])) + 1

    Lnum = np.zeros((n, n))
    Lnum[0] = -1                # Should do root-attachment features here.
    Lden = np.zeros((n, n))
    Lden[0] = -1                # Should do root-attachment features here.
    for r in c[1]:
        score = -np.exp(w[np.array(r.features.indices)].dot(r.features.values))
        if r.label == 1:
            Lnum[r.wid, r.wid2] = score
        else:
            Lnum[r.wid, r.wid2] = 0
        Lden[r.wid, r.wid2] = score
    Lnum += np.diag(-Lnum.sum(axis=0))
    Lden += np.diag(-Lden.sum(axis=0))

    invLnum = padUpleft(inv(Lnum[1:,1:]))
    invLden = padUpleft(inv(Lden[1:,1:]))

    fgrad = []
    for r in c[1]:
        mom = r.wid
        kid = r.wid2
        if mom == kid:
            mom = 0
        grad = -Lnum[mom, kid] * (invLnum[kid, mom] - invLnum[kid, kid]) + \
               Lden[mom, kid] * (invLden[kid, mom] - invLden[kid, kid])
        fgrad += [(long(f), float(grad * v)) for f, v in zip(r.features.indices, r.features.values)]

    return fgrad

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: cascade-features.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName='Cascade Features')
    sqlContext = SQLContext(sc)
    
    raw = sqlContext.read.load(sys.argv[1])
    df = raw.dropDuplicates(['cluster', 'series', 'date'])\
            .withColumn('day', datediff(col('date'), lit('1970-01-01')))\
            .na.drop(subset=['day'])\
            .select('cluster', 'series', 'date', 'day')\
            .rdd.groupBy(lambda r: r.cluster)\
            .flatMap(lambda c: numberWitnesses(c))\
            .toDF()

    df2 = df.select([col(x).alias(x + '2') for x in df.columns])

    gap = 730

    # We should use self-matches for root attachment
    pairs = df.join(df2, (df.cluster == df2.cluster2) \
                    & (df.wid != df2.wid2) & (colabs(df.day - df2.day2) < gap))

    getPairFeatures = udf(lambda series, series2, day, day2: pairFeatures(series, series2, day, day2),
                          ArrayType(StringType()))

    feats = pairs.withColumn('label', when(pairs.day2 > pairs.day, 1).otherwise(0))\
                 .withColumn('raw',
                             getPairFeatures(pairs.series, pairs.series2, pairs.day, pairs.day2))
    feats.cache()

    cv = CountVectorizer(inputCol='raw', outputCol='features', minDF=4.0)
    interner = cv.fit(feats)      # alternate possibility: grab features only from label==1 edges

    fcount = len(interner.vocabulary)
    w = np.zeros((fcount,))

    json.dump(interner.vocabulary, open(sys.argv[2], 'w'))

    full = interner.transform(feats)
    full.write.json(sys.argv[3])
    
    fdata = full.select('cluster', 'wid', 'wid2', 'label', 'features')\
                .rdd.groupBy(lambda r: r.cluster)
    fdata.cache()

    grad = fdata.flatMap(lambda c: featureGradients(c, w))\
                .toDF(['feat', 'grad']).groupBy('feat').agg(gsum('grad').alias('grad'))
    grad.write.json(sys.argv[4])
    
    sc.stop()
