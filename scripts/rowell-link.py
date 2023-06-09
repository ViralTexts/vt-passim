import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_set, lower, translate, explode, length, sort_array, struct, udf
import pyspark.sql.functions as f
from re import match, search, sub, split

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
            ).join(places.withColumnRenamed('div1', 'state'), ['series'], 'left_outer')

    rowell = spark.read.json(config.inputPath
                ).filter((col('country') != 'British Colonies.') & (col('country') != 'Dominion of Canada.')
                ).select('state', 'head', 'contents',
                         norm_title('title').alias('rtitle'),
                         explode(freq_guess('contents')).alias('freq'))
    
    # rowell.write.json(config.outputPath, mode='overwrite')
    rowell.join(usnd, (rowell.state == usnd.state) &
                (translate(lower('head'), " -'", '').startswith(translate(lower('city'), " -'", "")) | lower('head').startswith(col('rawPlace'))) &
                ((col('rtitle') == norm_title('title')) | (col('rtitle') == norm_title('alttitle'))), 'left_outer'
        ).drop(usnd.state
        ).withColumn('inStrict', (col('start') <= 1869) & (col('end') >= 1869)
        ).withColumn('inLax', (col('laxStart') <= 1869) & (col('laxEnd') >= 1869)
        ).withColumn('hasFreq', col('freq') == col('frequency')
        ).na.fill(False, subset=['inStrict', 'inLax', 'hasFreq']
        ).groupBy('state', 'head', 'contents', 'freq'
        ).agg(sort_array(collect_set(struct('hasFreq', 'inStrict', 'inLax', 'series', 'title', 'frequency', 'placeOfPublication', 'publisher', 'startDate', 'endDate')), asc=False).alias('matches')
        ).withColumn('isSafe', col('matches')[0]['hasFreq'] & col('matches')[0]['inLax']
        ).withColumn('guess', col('matches')[0]['series']
        ).select('state', 'head', 'contents', 'freq',
                 f.when(col('isSafe'), col('guess')).alias('series'),
                 'guess',
                 f.when(col('guess').isNotNull(),
                        f.concat(f.lit('https://chroniclingamerica.loc.gov'),'guess')).alias('url'),
                 f.when(~col('isSafe') & col('guess').isNotNull(),
                        f.to_json('matches')).alias('cand')
        ).sort('state', 'head'
        ).coalesce(1
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
