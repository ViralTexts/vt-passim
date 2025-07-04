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

def weightLCS(arr1, arr2, weights):
    matcher = difflib.SequenceMatcher(None, arr1, arr2)
    total = 0
    for match in matcher.get_opcodes():
        if match[0] == 'equal':
            total += sum(weights[match[1]:match[2]])
    return total
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CTS work overlap',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-o', '--overlap', type=float, default=0.05,
                        help='minimum overlap')
    parser.add_argument('docsPath', metavar='<docs path>', help='docs path')
    parser.add_argument('corpusPath', metavar='<corpus path>', help='corpus path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()
    
    rmruns = udf(lambda arr: noRuns(arr), 'array<string>')
    sum_runs = udf(lambda arr: sumRuns(arr), 'array<struct<loc: string, length: double>>')
    lcs = udf(lambda arr1, arr2: getLCS(arr1, arr2), 'array<string>')
    wlcs = udf(lambda arr1, arr2, weights: weightLCS(arr1, arr2, weights), 'double')

    corpus = spark.read.load(config.corpusPath, mergeSchema=True)

    ## We might need our own LCS that computes the maximum sum of the lengths of cited passages.

    spark.read.load(config.docsPath
        ).select('book', 'pos', explode('lines').alias('line')
        ).select('book', 'pos', col('line.begin'), explode('line.wits').alias('wit')
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
        ).join(corpus.select('id', 'locs', f.length('text').alias('tlen')
                    ).filter(size('locs') >= 20), 'id'
        ).withColumn('nlocs', size('locs')
        ).withColumn('cover', size(f.array_intersect('cites.loc', col('locs.loc'))) / col('nlocs')
        ).filter(col('cover') >= config.overlap
        ).withColumn('lcs', lcs(col('cites.loc'), col('locs.loc'))
        ).withColumn('lcslen', size('lcs')
        ).withColumn('overlap', col('lcslen') / col('nlocs')
        ).withColumn('wlcs', wlcs('cites.loc', 'locs.loc', 'cites.length')
        ).withColumn('wover', col('wlcs') / col('tlen')
        ).drop('locs'
        ).filter(col('overlap') >= config.overlap
        ).sort(f.desc('wover'), col('nlocs')
        ).write.json(config.outputPath, mode='overwrite')
    spark.stop()
