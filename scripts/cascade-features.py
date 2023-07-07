import argparse, os, re
import numpy as np

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, when, datediff, lit, sum as gsum
import pyspark.sql.functions as f
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, VectorAssembler, RFormula, SQLTransformer

def pairFeatures(sseries, dseries, sday, dday):
    lag = abs(dday - sday)      # throw away the sign to allow learning
    lagBin = str(int(np.log(lag))) if lag > 0 else '-inf'
    return ["src:" + sseries, "dst:" + dseries, "pair:" + sseries + ":" + dseries,
            "lag:" + lagBin]

def normalizeText(s):
    return re.sub("[^\w\s]", "", re.sub("\s+", " ", s.strip().lower()))

def clusterFeatures(c, gap):
    ## Sorting by string date needs to be consistent with numerical date
    wits = c[1]
    fields = ['cluster', 'src', 'dst', 'label', 'day', 'day2']
    info_keys = list(wits[0].info.asDict().keys())
    fields += info_keys
    fields += [(k + '2') for k in info_keys]
    Record = Row(*fields)
    n = len(wits)
    res = []
    curday = wits[0].day - gap
    prevday = curday
    for d in range(n):
        dst = wits[d]
        if dst.day > curday:
            prevday = curday
            curday = dst.day
        allowRoot = 1 if ( curday - prevday >= gap ) else 0
        dinfo = [dst[k] for k in info_keys]
        sinfo = [None for k in info_keys]
        res.append(Row(long(c[0]), 0, d+1, allowRoot, 0, dst.day, *sinfo, *dinfo))
        for s in range(n):
            src = wits[s]
            if (s != d) and (abs(dst.day - src.day) < gap):
                sinfo = [src[k] for k in info_keys]
                res.append(Row(long(c[0]), s+1, d+1,
                               (1 if dst.day > src.day else 0),
                               src.day, dst.day, *sinfo, *dinfo))
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

    try:
        numLgrad = laplaceGradient(numL)
    except:
        print('## num cluster=', c[0], '; size=', n)
        print(numL)
        exit(0)
        
    try:
        denLgrad = laplaceGradient(denL)
    except:
        print('## den cluster=', c[0], '; size=', n)
        print(denL)
        exit(0)

    fgrad = []
    for r in c[1]:
        mom = r.src
        kid = r.dst
        grad = -numL[mom, kid] * numLgrad[mom, kid] + denL[mom, kid] * denLgrad[mom, kid]
        fgrad += [(int(f), float(grad * v)) for f, v in zip(r.features.indices, r.features.values)]

    return fgrad

def dayGaps(wits, gap):
    res = []
    curday = wits[0].day - gap
    prevday = curday
    for w in wits:
        if w.day > curday:
            prevday = curday
            curday = w.day
        res.append(curday - prevday)
    return res

def featurizeData(raw, vocabFile, featFile, args):
    reprints = raw.withColumn('day', datediff(col('date'), lit('1970-01-01'))
                ).na.drop(subset=['day']
                ).dropDuplicates(['cluster', 'issue', 'day'])

    day_gaps = udf(lambda wits: dayGaps(wits, args.gap), 'array<int>')
   
    wits = SQLTransformer(statement= f'SELECT cluster, struct(day, series, struct({args.witness_fields}) AS info) AS wit FROM __THIS__'
                ).transform(reprints
                ).groupBy('cluster'
                ).agg(f.sort_array(f.collect_list('wit')).alias('wits')
                ).withColumn('wits', f.arrays_zip('wits', day_gaps('wits').alias('gap'))
                ).select('cluster', f.posexplode('wits')
                ).select('cluster', (col('pos')+1).alias('pos'),
                         'col.wits.day', 'col.gap', 'col.wits.info.*')

    f1 = wits.columns
    f2 = [f + ('2' if f != 'cluster' else '') for f in f1]

    pairs = wits.join(wits.toDF(*f2), 'cluster'
                ).withColumn('lag', col('day2') - col('day')
                ).filter(f.abs('lag') < args.gap
                ).withColumnRenamed('pos', 'src'
                ).withColumnRenamed('pos2', 'dst'
                ).withColumn('src', when(col('src') == col('dst'), lit(0)).otherwise(col('src'))
                ).withColumn('root', when(col('src') == 0, 1).otherwise(0)
                ).withColumn('label',
                             when(((col('src') == 0) & (col('gap2') >= args.gap)) |
                                  (col('day2') > col('day')),
                                  1).otherwise(0))

    for field in f1:
        if field != 'cluster' and field != 'pos':
            pairs = pairs.withColumn(field, when(col('src') == 0,args.root).otherwise(col(field)))

    stages = []
    if args.pair_fields:
        stages.append(SQLTransformer(statement=f'SELECT *, {args.pair_fields} FROM __THIS__'))
    stages.append(RFormula(formula=f'label ~ {args.formula}', handleInvalid='keep'))
    pipeline = Pipeline(stages=stages)

    pairs.cache()
    model = pipeline.fit(pairs)
    full = model.transform(pairs)

    names = np.empty(full.schema['features'].metadata['ml_attr']['num_attrs'], dtype='object')
    for k, v in full.schema['features'].metadata['ml_attr']['attrs'].items():
        for feat in v:
            names[feat['idx']] = feat['name']
    np.savetxt(vocabFile, names, fmt='%s')    

    full.withMetadata('features', {'ml_attr': {}}).write.save(featFile)
    pairs.unpersist()

    # dropDuplicates(['cluster', 'issue', 'date'])\
    #         .withColumn('day', datediff(col('date'), lit('1970-01-01')))\
    #         .na.drop(subset=['day'])\
    #         .rdd.groupBy(lambda r: r.cluster)\
    #         .flatMap(lambda c: clusterFeatures(c, gap))\
    #         .toDF()

    # feats.cache()
    # cv = CountVectorizer(inputCol='raw', outputCol='features', minDF=4.0)
    # interner = cv.fit(feats)      # alternate possibility: grab features only from label==1 edges
    # full = interner.transform(feats)
    # # combiner = VectorAssembler(inputCols=realCols + ['categorial'], outputCol='features')
    # # # I don't think a Pipeline will work here since we need to get the interner.vocabulary
    # # full = combiner.transform(interner.transform(feats)).drop('categorial')

    # full.write.parquet(featFile)
    # np.savetxt(vocabFile, np.array(interner.vocabulary), fmt='%s')
    # feats.unpersist()

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description='Cascade features')
    argparser.add_argument('-f', '--input', help='Input data')
    argparser.add_argument('-g', '--gap', type=int, default=730)
    argparser.add_argument('-i', '--iterations', type=int, default=20)
    argparser.add_argument('-r', '--rate', type=float, default=1.0)
    argparser.add_argument('-v', '--variance', type=float, default=1.0)
    argparser.add_argument('-p', '--posteriors', metavar='params')
    argparser.add_argument('--witness-fields', type=str, default='series')
    argparser.add_argument('--pair-fields', type=str, default=None)
    argparser.add_argument('--root', type=str, default='ROOT')
    argparser.add_argument('-s', '--formula', type=str, default='series * series2')

    argparser.add_argument('outdir', help='Output directory')
    args = argparser.parse_args()

    spark = SparkSession.builder.appName('Cascade Features').getOrCreate()
    
    vocabFile = args.outdir + "/vocab.gz"
    featFile = args.outdir + "/feats.parquet"

    if not os.path.exists(featFile):
        os.makedirs(args.outdir, exist_ok=True)
        featurizeData(spark.read.load(args.input), vocabFile, featFile, args)

    full = spark.read.load(featFile)
    vocab = np.loadtxt(vocabFile, 'str')

    if args.posteriors:
        w = np.loadtxt(args.posteriors)
        full.rdd.groupBy(lambda r: r.cluster).flatMap(lambda c: clusterPosteriors(c, w)).toDF()\
            .write.save(args.posteriors + ".parquet")
        spark.stop()
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
