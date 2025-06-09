import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, udf, regexp_replace, size, struct, udf, when)
import pyspark.sql.functions as f
from urllib.parse import unquote

def getTopdiv(hlabs, country, label):
    if hlabs == None or len(hlabs) < 1:
        return None
    cpos = -1
    for i in range(len(hlabs)):
        if hlabs[i] == country:
            cpos = i
    if cpos == -1:
        return hlabs[-1]
    elif cpos == 0:
        return label
    else:
        return hlabs[cpos-1]

def getRegion(country, topdiv):
    if topdiv in {'Hawaii', 'Puerto Rico', 'Alaska'}:
        return topdiv
    elif country == 'Dutch East Indies':
        return 'Indonesia'
    elif country == 'Republic of Ireland' or country == 'United Kingdom':
        return 'UK'
    # elif topdiv in {'Virginia', 'North Carolina', 'South Carolina', 'Georgia', 'Florida',
    #                 'Tennessee', 'Alabama', 'Mississippi', 'Arkansas', 'Louisiana', 'Texas'}:
    #     return 'CSA'
    elif topdiv in {'Washington', 'Oregon', 'California', 'Idaho', 'Wyoming', 'Nevada',
                    'Montana', 'Utah', 'Arizona', 'New Mexico', 'North Dakota', 'South Dakota',
                    'Nebraska', 'Kansas', 'Oklahoma', 'Texas'}:
        return 'WUSA'
    elif country == 'United States of America':
        return 'EUSA'
    else:
        return country

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Place Info',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('seriesPath', metavar='<path>', help='series path')
    parser.add_argument('dbpediaPath', metavar='<path>', help='dbpedia path')
    parser.add_argument('wikidataPath', metavar='<path>', help='wikidata path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Place Info').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    unesc = udf(lambda s: unquote(s))

    get_topdiv = udf(lambda hlabs, country, label: getTopdiv(hlabs, country, label))
    get_region = udf(lambda country, topdiv: getRegion(country, topdiv))

    series = spark.read.json(config.seriesPath)
    dbp = spark.read.load(config.dbpediaPath)
    wd = spark.read.load(config.wikidataPath
            ).filter(col('label').isNotNull()
            ).select(col('id').alias('wdid'), 'label', 'country', 'lon', 'lat',
                     f.array('litate').alias('hier'), f.array([]).alias('hlabs'))

    links = wd.select(col('wdid').alias('cid'), col('label').alias('clab'),
                      col('hier')[0].alias('litate'))

    cov = series.filter(col('coverage').isNotNull() #& col('corpora').isNotNull()
                ).select('coverage', unesc('coverage').alias('subject')).distinct()

    wdlinks = dbp.filter(col('object').startswith('http://www.wikidata.org/entity/')
                ).select('subject', regexp_replace('object', '^http://www.wikidata.org/entity/',
                                                   '').alias('wdid')
                ).filter("wdid <> 'Q87' OR subject <> 'http://dbpedia.org/resource/Alexandria,_Virginia'"
                ).filter("wdid <> 'Q1223' OR subject <> 'http://dbpedia.org/resource/Washington,_Arkansas'"
                ).filter("wdid <> 'Q129610' OR subject <> 'http://dbpedia.org/resource/Galway,_New_York'"
                ).filter("wdid <> 'Q179437' OR subject <> 'http://dbpedia.org/resource/Roscommon,_Michigan'")

    info = cov.join(wdlinks, ['subject'], 'left_outer'
            ).join(wd, ['wdid'], 'left_outer'
            ).withColumn('wnum', regexp_replace('wdid', '^Q', '').cast('int')
            ).groupBy('coverage'
            ).agg(f.min(struct('wnum', 'wdid', 'lon', 'lat', 'label', 'country', 'hier', 'hlabs')).alias('info')
            ).select('coverage', 'info.*'
            ).drop('wnum'
            ).join(links, [col('country') == col('cid')], 'left_outer'
            ).withColumn('country', col('clab')
            ).drop('cid', 'clab', 'litate')

    for i in range(7):
        info = info.join(links, [f.element_at('hier', -1) == col('cid')], 'left_outer'
                ).withColumn('hier', f.array_append('hier', col('litate'))
                ).withColumn('hlabs', f.array_append('hlabs', col('clab'))
                ).drop('cid', 'clab', 'litate')

    info.withColumn('hlabs', f.array_compact('hlabs')
        ).withColumn('topdiv', get_topdiv('hlabs', 'country', 'label')
        ).withColumn('region', get_region('country', 'topdiv')
        ).drop('hier', 'hlabs'
        ).sort('coverage'
        ).coalesce(1
#        ).write.json(config.outputPath, mode='overwrite')
        ).write.csv(config.outputPath, header=True, escape='"', mode='overwrite')

    spark.stop()
    
