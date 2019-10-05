from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf, substring, min, count, explode, lower, sum, desc, countDistinct, coalesce, regexp_replace
from pyspark.sql.types import DoubleType
from math import log

def zlog(x):
    if x == 0:
        return 0
    else:
        return log(x)

def dunningG(a, n1, n2, N):
    b = n1 - a
    c = n2 - a
    d = N - a - b - c
    return -2 * (a * (zlog(a) - log(N) - zlog(a/n1) - zlog(a/n2)) +
                 b * (zlog(b) - log(N) - zlog(b/(b+d)) - zlog(b/n1)) +
                 c * (zlog(c) - log(N) - zlog(c/(c+d)) - zlog(c/n2)) +
                 d * (zlog(d) - log(N) - zlog(d/(b+d)) - zlog(d/(c+d))))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: term-spikes.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Rank term spikes').getOrCreate()

    corpus_lang = {'bsb': 'de', 'ca': 'en', 'ddd': 'nl', 'gale-uk': 'en',
                   'gale-us': 'en', 'moa': 'en', 'onb': 'de',
                   'sbb': 'de', 'tda': 'en', 'trove': 'en', 'vac': 'en'}

    lang_norm = { 'English': 'en', 'eng': 'en', 'ger': 'de', 'German': 'de',
                  'fre': 'fr', 'French': 'fr', 'Russian': 'ru',
                  'Croatian': 'hr' }
    
    ecorp = ['ca', 'moa', 'trove', 'aps', 'gale-us', 'gale-uk', 'tda']

    stops = ['january', 'february', 'march', 'april', 'may', 'june', 'july',
             'august', 'september', 'october', 'november', 'december', 'christmas',
             'januar', 'februar', 'maart', 'junij', 'julij', 'oktober', 'dezember',
             'weihnachts', 'weihnachten']

    clusters = spark.read.load(sys.argv[1]) \
                         .withColumn('corpus_lang', col('corpus')) \
                         .na.replace(corpus_lang.keys(), corpus_lang.values(), 'corpus_lang') \
                            .replace('', None, 'lang') \
                        .withColumn('lang', regexp_replace(coalesce('doc_lang', 'lang', 'corpus_lang'), ',.*$', '')) \
                        .na.replace(lang_norm.keys(), lang_norm.values(), 'lang')

    months = clusters.filter(col('date').rlike('^\\d{4}-\\d\\d-'))\
                     .select(col('cluster'), substring('date', 0, 7).alias('month')) \
                     .groupBy('cluster').agg(min('month').alias('month'))

    mocounts = months.groupBy('month').agg(count('cluster').alias('clusters'))

    N = mocounts.select(sum('clusters').alias('total')).collect()[0]['total']

    regexTokenizer = RegexTokenizer(inputCol='text', outputCol='words', pattern='[A-Z]\\w+',
                                    gaps=False, toLowercase=False, minTokenLength=5)

    words = \
        regexTokenizer \
        .transform(clusters) \
        .select('cluster', 'corpus', 'lang', explode('words').alias('word')) \
        .withColumn('word', lower(col('word'))) \
        .filter(~col('word').isin(stops)) \
        .distinct() \
        .withColumn('inen', col('corpus').isin(ecorp).cast('int')) \
        .withColumn('innonen', (~col('corpus').isin(ecorp)).cast('int')) \
        .join(months, 'cluster') \
        .groupBy('month', 'word') \
        .agg(countDistinct('cluster').alias('freq'),
             sum('inen').alias('ecount'), sum('innonen').alias('nonecount')) \
        .filter( (col('ecount') > 1) & (col('nonecount') > 1) ) \

    totwords = words.groupBy('word').agg(sum('freq').alias('total'))

    gstat = udf(lambda a, n1, n2: dunningG(a, n1, n2, N), DoubleType())

    words \
        .join(totwords, 'word') \
        .join(mocounts, 'month') \
        .withColumn('loglike', gstat(col('freq'), col('clusters'), col('total'))) \
        .sort(desc('loglike')) \
        .write.csv(sys.argv[2], header=True)

    spark.stop()

