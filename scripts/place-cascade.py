from __future__ import print_function

import argparse, re
import numpy as np

from pyspark import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, datediff, lit, sum as gsum
from pyspark.ml.feature import CountVectorizer, VectorAssembler

def pairFeatures(sseries, dseries, sday, dday):
    lag = abs(dday - sday)      # throw away the sign to allow learning
    lagBin = str(int(np.log(lag))) if lag > 0 else '-inf'
    return ["src:" + sseries, "dst:" + dseries, "pair:" + sseries + ":" + dseries,
            "lag:" + lagBin]

def normalizeText(s):
    return re.sub("[^\w\s]", "", re.sub("\s+", " ", s.strip().lower()))

def clusterFeatures(c, gap):
    ## Sorting by string date needs to be consistent with numerical date
    wits = sorted(c[1], key=lambda w: w.date + ' ' + w.series)
    n = len(wits)
    res = []
    curday = wits[0].day - gap
    prevday = curday
    for d in range(n):
        dst = wits[d]
        dstClean = normalizeText(dst.text)
        if dst.day > curday:
            prevday = curday
            curday = dst.day
        allowRoot = 1 if ( curday - prevday >= gap ) else 0
        res.append(Row(cluster=long(c[0]), src=0, dst=d+1, label=allowRoot,
                       longer=0.0, shorter=0.0,
                       raw=["root:" + dst.series]))
        for s in range(n):
            src = wits[s]
            if (s != d) and (abs(dst.day - src.day) < gap):
                srcClean = normalizeText(src.text)
                growth = (len(dstClean) - len(srcClean))/float(len(srcClean))
                res.append(Row(cluster=long(c[0]), src=s+1, dst=d+1,
                               label=(1 if dst.day > src.day else 0),
                               longer=growth if growth > 0 else 0.0,
                               shorter=abs(growth) if growth < 0 else 0.0,
                               raw=pairFeatures(src.series, dst.series, src.day, dst.day)))
    return res

# Pad upper left row/col with zeros.
def padUpLeft(m):
    size = m.shape[0]
    return np.concatenate((np.zeros((size+1, 1)),
                           np.concatenate((np.zeros((1, size)), m), axis=0)),
                          axis=1)

def laplaceGradient(L):
    tinv = padUpLeft(np.transpose(np.linalg.inv(L[1:, 1:])))
    return tinv - tinv.diagonal()

## Should figure out how to reuse this in clusterGradients
def clusterPosteriors(c, w):
    n = max(map(lambda r: r.dst, c[1])) + 1

    L = np.zeros((n, n))
    for r in c[1]:
        score = -np.exp(w[np.array(r.features.indices)].dot(r.features.values))
        L[r.src, r.dst] = score if r.label == 1 else 0
    L += np.diag(-L.sum(axis=0))

    Lgrad = laplaceGradient(L)

    posts = []
    for r in c[1]:
        mom = r.src
        kid = r.dst
        post = L[mom, kid] * Lgrad[mom, kid]
        posts.append(Row(post=float(post), **(r.asDict())))
    return posts

def clusterGradients(c, w):
    n = max(map(lambda r: r.dst, c[1])) + 1

    numL = np.zeros((n, n))
    denL = np.zeros((n, n))
    for r in c[1]:
        score = -np.exp(w[np.array(r.features.indices)].dot(r.features.values))
        numL[r.src, r.dst] = score if r.label == 1 else 0
        denL[r.src, r.dst] = score
    numL += np.diag(-numL.sum(axis=0))
    denL += np.diag(-denL.sum(axis=0))

    numLgrad = laplaceGradient(numL)
    denLgrad = laplaceGradient(denL)

    fgrad = []
    for r in c[1]:
        mom = r.src
        kid = r.dst
        grad = -numL[mom, kid] * numLgrad[mom, kid] + denL[mom, kid] * denLgrad[mom, kid]
        fgrad += [(long(f), float(grad * v)) for f, v in zip(r.features.indices, r.features.values)]

    return fgrad

def featurizeData(raw, gap, vocabFile, featFile):
    feats = raw.dropDuplicates(['cluster', 'series', 'date'])\
            .withColumn('day', datediff(col('date'), lit('1970-01-01')))\
            .na.drop(subset=['day'])\
            .rdd.groupBy(lambda r: r.cluster)\
            .flatMap(lambda c: clusterFeatures(c, gap))\
            .toDF()

    feats.cache()
    cv = CountVectorizer(inputCol='raw', outputCol='features', minDF=4.0)
    interner = cv.fit(feats)      # alternate possibility: grab features only from label==1 edges
    full = interner.transform(feats)
    # combiner = VectorAssembler(inputCols=realCols + ['categorial'], outputCol='features')
    # # I don't think a Pipeline will work here since we need to get the interner.vocabulary
    # full = combiner.transform(interner.transform(feats)).drop('categorial')

    full.write.parquet(featFile)
    np.savetxt(vocabFile, np.array(interner.vocabulary), fmt='%s')
    feats.unpersist()

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description='Cascade features')
    argparser.add_argument('-f', '--input', help='Input data')
    argparser.add_argument('-g', '--gap', type=int, default=730)
    argparser.add_argument('-i', '--iterations', type=int, default=20)
    argparser.add_argument('-r', '--rate', type=float, default=1.0)
    argparser.add_argument('-v', '--variance', type=float, default=1.0)
    argparser.add_argument('-p', '--posteriors', metavar='params')
    argparser.add_argument('outdir', help='Output directory')
    args = argparser.parse_args()

    spark = SparkSession.builder.appName('Cascade Features').getOrCreate()
    
    vocabFile = args.outdir + "/vocab.gz"
    featFile = args.outdir + "/feats.parquet"

    try:
        full = spark.read.load(featFile)
        vocab = np.loadtxt(vocabFile, 'string')
    except:
        featurizeData(spark.read.load(args.input), args.gap, vocabFile, featFile)
        full = spark.read.load(featFile)
        vocab = np.loadtxt(vocabFile, 'string')

    if args.posteriors:
        w = np.loadtxt(args.posteriors)
        full.rdd.groupBy(lambda r: r.cluster).flatMap(lambda c: clusterPosteriors(c, w)).toDF()\
            .write.save(args.posteriors + ".parquet")
        exit(0)

    fcount = len(vocab)
    w = np.zeros(fcount)
    
    fdata = full.select('cluster', 'src', 'dst', 'label', 'features')\
                .rdd.groupBy(lambda r: r.cluster)
    fdata.cache()

    rate = args.rate / fdata.count()            # scale with training size

    for i in range(args.iterations):
        grad = fdata.flatMap(lambda c: clusterGradients(c, w)).toDF(['feat', 'grad'])\
                    .groupBy('feat').agg(gsum('grad').alias('grad'))\
                    .collect()
        update = np.zeros(fcount)
        for g in grad:
            update[g.feat] = g.grad
        if args.variance > 0:
            update += w / args.variance
        w -= rate * update
        np.savetxt("%s/iter%03d.gz" % (args.outdir, i), w)
    
    spark.stop()
