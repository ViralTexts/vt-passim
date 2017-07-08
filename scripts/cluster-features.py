from __future__ import print_function

import argparse, re
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, first, regexp_replace, udf
from pyspark.ml.feature import CountVectorizer, RegexTokenizer
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.ml.clustering import LDA

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
    return SparseVector(x.size, x.indices, map(lambda v: v * k, x.values))

def threshold_sparse(x, t):
    return SparseVector(x.size, [(k, v) for (k, v) in zip(x.indices, x.values) if v >= t])

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description='Cluster features')
    argparser.add_argument('-c', '--minCount', type=float, default=1.0)
    argparser.add_argument('-s', '--clusterSize', type=int, default=1)
    argparser.add_argument('indir', help='Input directory')
    argparser.add_argument('outdir', help='Output directory')
    args = argparser.parse_args()

    spark = SparkSession.builder.appName('Cluster Features').getOrCreate()

    df = spark.read.load(args.indir)

    raw = df.filter(col('size') >= args.clusterSize) \
            .select('cluster', 'size', regexp_replace('text', u'\xad\s*', '').alias('text'))
    raw.cache()

    tok = RegexTokenizer(inputCol='text', outputCol='terms', gaps=False, pattern='\w+') \
          .transform(raw)
    vocabFeaturizer = CountVectorizer(inputCol='terms', outputCol='counts', minDF=2.0).fit(tok)
    counts = vocabFeaturizer.transform(tok)
    
    mergeCounts = udf(lambda va, size: threshold_sparse(scale_sparse(reduce(add_sparse, va), 1.0/size), args.minCount),
                      VectorUDT())

    counts.groupBy('cluster', 'size') \
          .agg(mergeCounts(collect_list('counts'), 'size').alias('counts')) \
          .write.parquet(args.outdir)

    vocab = vocabFeaturizer.vocabulary
    spark.createDataFrame(zip(range(len(vocab)), vocab), ['id', 'word']) \
         .write.json(args.outdir + "-vocab")

    # lda = LDA(k=2, featuresCol='counts', seed=1, optimizer='em')
    # model = lda.fit(res)
    # model.describeTopics().write.json(args.outdir)

    spark.stop()
