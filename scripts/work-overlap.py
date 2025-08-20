import argparse, difflib
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, sort_array, struct, explode, udf, flatten, size
import pyspark.sql.functions as f

def noRuns(arr):
    cur = None
    res = []
    for s in arr:
        if s != cur:
            res.append(s)
            cur = s
    return res

def sumRuns(arr):
    cur = None
    total = 0
    res = []
    for r in arr:
        if r.loc != cur:
            if cur != None:
                res.append((cur, total))
            cur = r.loc
            total = 0
        total += r.length
    if cur != None:
        res.append((cur, total))
    return res
        
def getLCS(arr1, arr2):
    matcher = difflib.SequenceMatcher(None, arr1, arr2)
    lcs = []
    for match in matcher.get_opcodes():
        if match[0] == 'equal':
            lcs.extend(arr1[match[1]:match[2]])
    return lcs

def lbLCS(arr1, arr2, weights):
    matcher = difflib.SequenceMatcher(None, arr1, arr2)
    total = 0
    for match in matcher.get_opcodes():
        if match[0] == 'equal':
            total += sum(weights[match[1]:match[2]])
    return total

def weightLCS(arr1, arr2, thresh=0.9):
    best = {}
    for r in arr1:
        best[r.loc] = max(best.get(r.loc, 0), r.length)
    locs1 = []
    weights = []
    for r in arr1:
        if r.length >= (best[r.loc] * thresh):
            locs1.append(r.loc)
            weights.append(r.length)
    locs2 = [r.loc for r in arr2]
    return lbLCS(locs1, locs2, weights)

    # chart = dict()
    # for i in range(len(arr1)+1):
    #     chart[i] = dict()
    #     chart[i][0] = 0.0
    # for j in range(len(arr2)+1):
    #     chart[0][j] = 0.0
    # for i in range(len(arr1)):
    #     for j in range(len(arr2)):
    #         if arr1[i] == arr2[j]:
    #             chart[i+1][j+1] = chart[i][j] + weights[i]
    #         else:
    #             chart[i+1][j+1] = max(chart[i+1][j], chart[i][j+1])
    # return chart[len(arr1)][len(arr2)]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CTS work overlap',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-o', '--overlap', type=float, default=0.01,
                        help='minimum overlap')
    parser.add_argument('docsPath', metavar='<docs path>', help='docs path')
    parser.add_argument('corpusPath', metavar='<corpus path>', help='corpus path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()
    
    rmruns = udf(lambda arr: noRuns(arr), 'array<string>')
    sum_runs = udf(lambda arr: sumRuns(arr), 'array<struct<loc: string, length: double>>')
    lcs = udf(lambda arr1, arr2: getLCS(arr1, arr2), 'array<string>')
    lblcs = udf(lambda arr1, arr2, weights: lbLCS(arr1, arr2, weights), 'double')
    wlcs = udf(lambda arr1, arr2: weightLCS(arr1, arr2), 'double')

    corpus = spark.read.load(config.corpusPath, mergeSchema=True)
    cinfo = corpus.groupBy(col('book').alias('edition')
                 ).agg(f.sum(f.length('text')).alias('tlen'),
                       f.sum(size('locs')).alias('nlocs')
                 ).filter( (col('tlen') >= 1000) )

    spark.read.load(config.docsPath
        ).select('book', 'pos', explode('lines').alias('line')
        ).select('book', 'pos', col('line.begin'), explode('line.wits').alias('wit')
        ).filter(col('wit.matches')/f.length('wit.text') >= 0.1
        # ).select('book', 'pos', 'begin', col('wit.id'), col('wit.locs')
        ).select('book', 'pos', 'begin', col('wit.id'),
                 f.transform('wit.locs',
                             lambda r: struct(r.alias('loc'), (col('wit.matches')
                                                               /size('wit.locs')).alias('length'))
                             ).alias('locs')
        ).filter(col('locs').isNotNull() & (size('locs') > 0)
        ).groupBy('book', 'pos', 'id'
        ).agg(sum_runs(flatten(sort_array(collect_list(struct('begin', 'locs')))['locs'])).alias('locs')
        ).groupBy('book', 'id'
        ).agg(sum_runs(flatten(sort_array(collect_list(struct('pos',
                                                              'locs')))['locs'])).alias('cites')
        ).filter(size('cites') > 1
        ).join(corpus.select('id', 'locs', col('book').alias('edition')
                            ).filter(f.size('locs') > 1), 'id'
        ).withColumn('covered', size(f.array_intersect('cites.loc', col('locs.loc')))
        ).withColumn('lcs', lcs(col('cites.loc'), col('locs.loc'))
        ).withColumn('lcslen', size('lcs')
        ).withColumn('lblcs', lblcs('cites.loc', 'locs.loc', 'cites.length')
        ).withColumn('wlcs', wlcs('cites', 'locs')
        ).drop('locs', 'cites', 'lcs'
        ).groupBy('edition', 'book'
        ).agg(f.sum('covered').alias('covered'),
              f.sum('lcslen').alias('lcslen'),
              f.sum('lblcs').alias('lblcs'),
              f.sum('wlcs').alias('wlcs')
        ).join(cinfo, 'edition'
        ).withColumn('cover', col('covered') / col('nlocs')
        ).filter(col('cover') >= config.overlap
        ).withColumn('overlap', col('lcslen') / col('nlocs')
        ).withColumn('wover', col('wlcs') / col('tlen')
        ).drop('covered'
        ).filter(col('wover') >= config.overlap
        ).repartition(1
        ).sort(f.desc('wover'), col('nlocs')
        ).write.csv(config.outputPath, mode='overwrite', header=True, escape='"')
    spark.stop()
