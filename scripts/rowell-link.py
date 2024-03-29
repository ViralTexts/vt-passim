import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_set, lower, translate, explode, length, sort_array, struct, udf
import pyspark.sql.functions as f
from re import match, search, sub, split
import re
from word2number import w2n

def normTitle(s):
    if s == None or s == '':
        return s
    return sub(r'[\s\-]+', ' ',
               sub(r'\b(daily|(tri-?)?weekly)\b', '',
                   sub(r'^the\s+', '',
                       s.lower().strip(' .,').replace("'", '')))).strip()

def freqGuess(contents):
    if contents == None or contents == '':
        return ['']
    m = match(r'[A-Za-z\-]+', contents)
    full = ''
    if m:
        s = m[0]
        if s == 'Every':
            full = 'daily'
        elif match(r'^[A-Z][a-z]+days$', s):
            full = 'weekly'
        else:
            full = s.lower()
    m = search(r'— ([A-Za-z\-]+ [^;]+);', contents)
    if m:
        return [s.split()[0] for s in split(r',\s*', m[1])]
    else:
        return [full]

def getCirc(contents, freq):
    if contents == None or contents == '':
        return None
    m = search(' ' + freq + ' (([0-9]{1,3},)?[0-9]{3}\\b)', contents, re.I)
    if m:
        return m[1].replace(',', '')
    m = search(' (([0-9]{1,3})?[0-9]{3}) ' + freq, contents, re.I)
    if m:
        return m[1].replace(',', '')
    m = search(r'\b(([0-9]{1,3},)?[0-9]{3})\b', contents, re.I)
    if m:
        return m[1].replace(',', '')
    return None

def getEstablished(contents, freq):
    if contents == None or contents == '':
        return None
    m = search(' ' + freq + ' (\\d{4})\\b', contents, re.I)
    if m:
        return m[1]
    m = search(' (\\d{4}) ' + freq, contents, re.I)
    if m:
        return m[1]
    m = search(r'\b(\d{4})\b', contents, re.I)
    if m:
        return m[1]
    return None

def getPages(contents, freq):
    if contents == None or contents == '':
        return None
    m = search(' ' + freq + ' (\\S+( hundred)?) pages', contents, re.I)
    if m:
        return m[1].lower()
    m = search(' (\S+( hundred)?) pages ' + freq, contents, re.I)
    if m:
        return m[1].lower()
    m = search(r'\b(\S+( hundred)?) pages', contents, re.I)
    if m:
        return m[1].lower()
    return None

def getSubscription(contents, freq):
    if contents == None or contents == '':
        return None
    m = search(' ' + freq + ' (\\$\\d+(\\.\\d\\d)?)', contents, re.I)
    if m:
        return m[1]
    m = search(' (\\$\\d+(\\.\\d\\d)?) ' + freq, contents, re.I)
    if m:
        return m[1]
    m = search(r'(\$\d+(\.\d\d)?)\b', contents, re.I)
    if m:
        return m[1]
    return None

def getSize(contents, freq):
    if contents == None or contents == '':
        return None
    m = search(' ' + freq + ' (\\d+x\\d+)', contents, re.I)
    if m:
        return m[1]
    m = search(' (\\d+x\\d+) ' + freq, contents, re.I)
    if m:
        return m[1]
    m = search(r'size (\S+)\b', contents, re.I)
    if m:
        return m[1]
    return None

def numConvert(s):
    res = None
    try:
        res = w2n.word_to_num(s)
    except:
        res = None
    return res
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Link Rowell records',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('marcPath', metavar='<input path>', help='marc path')
    parser.add_argument('placePath', metavar='<input path>', help='place path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Patch Alto').getOrCreate()

    norm_title = udf(lambda s: normTitle(s))
    freq_guess = udf(lambda contents: freqGuess(contents), 'array<string>')
    get_circ = udf(lambda contents, freq: getCirc(contents, freq))
    get_established = udf(lambda contents, freq: getEstablished(contents, freq))
    get_pages = udf(lambda contents, freq: numConvert(getPages(contents, freq)), 'int')
    get_subscription = udf(lambda contents, freq: getSubscription(contents, freq))
    get_size = udf(lambda contents, freq: getSize(contents, freq))

    marc = spark.read.csv(config.marcPath, header=True)
    places = spark.read.csv(config.placePath, header=True)

    usnd = marc.select('series', 'title', 'alttitle', 'startDate', 'endDate', 'start', 'end',
                       'frequency', 'placeOfPublication', 'publisher',
                       f.regexp_replace(lower('placeOfPublication'),
                                        r'^([a-z ]*[a-z]).*$', '$1').alias('rawPlace'),
                       (translate('startDate', 'u', '0').cast('int')).alias('laxStart'),
                       (translate('endDate', 'u', '9').cast('int')).alias('laxEnd')
            ).filter('(laxStart - 10) <= 1869 AND (laxEnd + 10) >= 1869'
            ).replace(['Semimonthly', 'Semiweekly', 'Three times a week'],
                      ['Semi-Monthly', 'Semi-Weekly', 'Tri-Weekly'],
                      'frequency'
            ).withColumn('frequency', lower('frequency')
            ).join(places.withColumnRenamed('div1', 'state'), ['series'], 'left_outer'
            ).drop('country')

    rowell = spark.read.json(config.inputPath
                ).select('country', 'state', 'head', 'contents',
                         f.monotonically_increasing_id().alias('uid'),
                         norm_title('title').alias('rtitle'),
                         explode(freq_guess('contents')).alias('freq')
                ).withColumn('circulation', get_circ('contents', 'freq')
                ).withColumn('established', get_established('contents', 'freq')
                ).withColumn('pages', get_pages('contents', 'freq')
                ).withColumn('size', get_size('contents', 'freq')
                ).withColumn('subscription', get_subscription('contents', 'freq'))
    
    rowell.join(usnd, (rowell.state == usnd.state) &
                (translate(lower('head'), " -'", '').startswith(translate(lower('city'), " -'", "")) | lower('head').startswith(col('rawPlace'))) &
                ((col('rtitle') == norm_title('title')) | (col('rtitle') == norm_title('alttitle'))), 'left_outer'
        ).drop(usnd.state
        ).withColumn('inStrict', (col('start') <= 1869) & (col('end') >= 1869)
        ).withColumn('inLax', (col('laxStart') <= 1869) & (col('laxEnd') >= 1869)
        ).withColumn('hasFreq', col('freq') == col('frequency')
        ).na.fill(False, subset=['inStrict', 'inLax', 'hasFreq']
        ).groupBy('uid', 'country', 'state', 'head', 'contents', 'freq', 'circulation',
                  'established', 'pages', 'size', 'subscription',
        ).agg(sort_array(collect_set(struct('hasFreq', 'inStrict', 'inLax', 'series', 'title', 'frequency', 'placeOfPublication', 'publisher', 'startDate', 'endDate')), asc=False).alias('matches')
        ).withColumn('isSafe', (col('matches')[0]['hasFreq'] & col('matches')[0]['inLax'])
#                     ((col('freq') == '') & (f.size('matches') == 1) & col('matches')[0]['inStrict'])
        ).withColumn('guess', col('matches')[0]['series']
        ).select('uid', 'country', 'state', 'head', 'contents', col('freq').alias('edition'),
                 'pages', 'size', 'subscription', 'established', 'circulation',
                 f.when(col('isSafe'), col('guess')).alias('series'),
                 'guess',
                 f.when(col('guess').isNotNull(),
                        f.concat(f.lit('https://chroniclingamerica.loc.gov'),'guess')).alias('url'),
                 f.when(~col('isSafe') & col('guess').isNotNull(),
                        f.to_json('matches')).alias('cand')
        ).sort('uid'
        ).drop('uid'
        ).coalesce(1
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')
#        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
