from __future__ import print_function

import json
import sys
import numpy as np
from numpy.linalg import inv
from math import log

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col, datediff, lit, udf, when, desc, sum as gsum
from pyspark.ml.feature import CountVectorizer

def pairFeatures(sseries, dseries, sday, dday):
    lag = abs(dday - sday)      # throw away the sign to allow learning
    lagBin = str(int(log(lag))) if lag > 0 else '-inf'
    return ["src:" + sseries, "dst:" + dseries, "pair:" + sseries + ":" + dseries,
            "lag:" + lagBin]

def clusterFeatures(c, gap):
    wits = sorted(c[1], key=lambda w: w.date + ':' + w.series)
    n = len(wits)
    res = []
    for d in range(n):
        dst = wits[d]
        res.append(Row(cluster=long(c[0]), src=0, dst=d+1, label=1,
                       raw=["root", "root:" + dst.series]))
        for s in range(n):
            src = wits[s]
            if (s != d) and (abs(dst.day - src.day) < gap):
                res.append(Row(cluster=long(c[0]), src=s+1, dst=d+1,
                               label=(1 if dst.day > src.day else 0),
                               raw=pairFeatures(src.series, dst.series, src.day, dst.day)))
    return res

def padUpleft(m):
    size = m.shape[0]
    return np.concatenate((np.zeros((size+1, 1)),
                           np.concatenate((np.zeros((1, size)), m), axis=0)),
                          axis=1)

def featureGradients(c, w):
    n = max(map(lambda r: r.dst, c[1])) + 1

    numL = np.zeros((n, n))
    denL = np.zeros((n, n))
    for r in c[1]:
        score = -np.exp(w[np.array(r.features.indices)].dot(r.features.values))
        numL[r.src, r.dst] = score if r.label == 1 else 0
        denL[r.src, r.dst] = score
    numL += np.diag(-numL.sum(axis=0))
    denL += np.diag(-denL.sum(axis=0))

    numLinv = padUpleft(inv(numL[1:,1:]))
    denLinv = padUpleft(inv(denL[1:,1:]))

    fgrad = []
    for r in c[1]:
        mom = r.src
        kid = r.dst
        grad = -numL[mom, kid] * (numLinv[kid, mom] - numLinv[kid, kid]) + \
               denL[mom, kid] * (denLinv[kid, mom] - denLinv[kid, kid])
        fgrad += [(long(f), float(grad * v)) for f, v in zip(r.features.indices, r.features.values)]

    return fgrad

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: cascade-features.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName='Cascade Features')
    sqlContext = SQLContext(sc)
    
    gap = 730

    raw = sqlContext.read.load(sys.argv[1])
    feats = raw.dropDuplicates(['cluster', 'series', 'date'])\
            .withColumn('day', datediff(col('date'), lit('1970-01-01')))\
            .na.drop(subset=['day'])\
            .rdd.groupBy(lambda r: r.cluster)\
            .flatMap(lambda c: clusterFeatures(c, gap))\
            .toDF()

    feats.cache()
    cv = CountVectorizer(inputCol='raw', outputCol='features', minDF=4.0)
    interner = cv.fit(feats)      # alternate possibility: grab features only from label==1 edges

    fcount = len(interner.vocabulary)
    w = np.zeros((fcount,))

    json.dump(interner.vocabulary, open(sys.argv[2], 'w'))

    full = interner.transform(feats)
    full.write.json(sys.argv[3])
    
    fdata = full.select('cluster', 'src', 'dst', 'label', 'features')\
                .rdd.groupBy(lambda r: r.cluster)
    fdata.cache()

    grad = fdata.flatMap(lambda c: featureGradients(c, w))\
                .toDF(['feat', 'grad']).groupBy('feat').agg(gsum('grad').alias('grad'))
    grad.write.json(sys.argv[4])
    
    sc.stop()
