import argparse, os, re
import numpy as np
import pandas as pd
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, first, regexp_replace, udf
import pyspark.sql.functions as f
from pyspark.ml.feature import CountVectorizer, RegexTokenizer
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.regression import LinearRegression, LinearRegressionModel

## Can you !@#$% believe there's no addition function for sparse
## vectors?  SparseVector class guarantees sorted indices
def add_sparse(x, y):
    xn = len(x.indices)
    yn = len(y.indices)
    idx = []
    val = []
    i = j = 0
    while i < xn or j < yn:
        if j >= yn or (i < xn and x.indices[i] < y.indices[j]):
            idx.append(x.indices[i])
            val.append(x.values[i])
            i += 1
        elif i >= xn or y.indices[j] < x.indices[i]:
            idx.append(y.indices[j])
            val.append(y.values[j])
            j += 1
        elif x.indices[i] == y.indices[j]:
            idx.append(x.indices[i])
            val.append(x.values[i] + y.values[j])
            i += 1
            j += 1
    return SparseVector(x.size, idx, val)

def scale_sparse(x, k):
    return SparseVector(x.size, x.indices, list(map(lambda v: v * k, x.values)))

def threshold_sparse(x, t):
    return SparseVector(x.size, [(k, v) for (k, v) in zip(x.indices, x.values) if v >= t])

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description='Cluster features')
    argparser.add_argument('-b', '--begin-year', type=int, default=1840)
    argparser.add_argument('-c', '--min-count', type=float, default=0.1)
    argparser.add_argument('-r', '--max-returnp', type=float, default=0.2)
    argparser.add_argument('-s', '--cluster-size', type=int, default=10)
    argparser.add_argument('--regParam', type=float, default=0.001)
    argparser.add_argument('indir', help='Input directory')
    argparser.add_argument('perioddir', help='Period directory')
    argparser.add_argument('outdir', help='Output directory')
    args = argparser.parse_args()

    spark = SparkSession.builder.appName(argparser.description).getOrCreate()

    featdir = args.outdir + '/feats.parquet'
    vocabFile = args.outdir + '/vocab.gz'

    if not os.path.exists(featdir):
        raw = spark.read.load(args.indir
                ).select('syear', 'cluster', regexp_replace('text', u'\xad\s*', '').alias('text')
                ).filter(col('syear') >= args.begin_year)

        clusters = spark.read.load(args.perioddir
                    ).withColumn('returnp', col('nreturn') / col('issues')
                    ).filter(col('returnp') <= args.max_returnp
                    ).filter(col('issues') >= args.cluster_size
                    ).select('cluster', 'issues', 'returnp', 'lag75p'
                    ).join(raw, 'cluster')

        tok = RegexTokenizer(inputCol='text', outputCol='terms', gaps=False, pattern='\w+'
                ).transform(clusters)
        vocabFeaturizer = CountVectorizer(inputCol='terms', outputCol='counts', minDF=1.0).fit(tok)
        counts = vocabFeaturizer.transform(tok)
    
        mergeCounts = udf(lambda va, size: threshold_sparse(scale_sparse(reduce(add_sparse, va), 1.0/size), args.min_count),
                          VectorUDT())

        counts.groupBy('syear', 'cluster', 'issues', 'lag75p'
            ).agg(mergeCounts(collect_list('counts'), 'issues').alias('counts')
            ).withColumn('label', (col('lag75p') > 31).cast('double')
            ).repartition('syear'
            ).write.partitionBy('syear').parquet(featdir)

        vocab = vocabFeaturizer.vocabulary
        np.savetxt(vocabFile, vocab, fmt='%s')

    feats = spark.read.load(featdir)
    vocab = np.loadtxt(vocabFile, 'str', delimiter='\t')

    syears = [r.syear for r in feats.select('syear').distinct().sort('syear').collect()]

    cparams = []
    rparams = []

    for y in syears:
        print(y)
        shard = feats.filter(col('syear') == y).withColumn('resp', f.log1p('lag75p'))
        shard.cache()
        lr = LogisticRegression(regParam=args.regParam,
                                featuresCol='counts', standardization=False)
        model = lr.fit(shard)
        mfile = '%s/c%d' % (args.outdir, y)
        print(mfile)
        model.save(mfile)
        cparams.append(np.insert(model.coefficients.toArray(), 0, model.intercept))

        lr = LinearRegression(regParam=args.regParam,
                              featuresCol='counts', labelCol='resp', standardization=False)
        model = lr.fit(shard)
        mfile = '%s/r%d' % (args.outdir, y)
        print(mfile)
        model.save(mfile)
        rparams.append(np.insert(model.coefficients.toArray(), 0, model.intercept))

        shard.unpersist()

    df = pd.DataFrame(np.column_stack(cparams),
                      columns=['s' + str(y) for y in syears],
                      index=np.insert(vocab, 0, '(Intercept)'))
    df.to_csv(args.outdir + '/params.csv', float_format='%.6f')
        
    df = pd.DataFrame(np.column_stack(rparams),
                      columns=['s' + str(y) for y in syears],
                      index=np.insert(vocab, 0, '(Intercept)'))
    df.to_csv(args.outdir + '/rparams.csv', float_format='%.6f')

    spark.stop()
