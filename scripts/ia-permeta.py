import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, udf)
import pyspark.sql.functions as f

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
          'September', 'October', 'November', 'December']

momap = dict(zip(months, range(1, 13)))

momap['Janaury'] = 1
momap['Januaury'] = 1
momap['Jan.'] = 1

mopat = '|'.join(momap.keys())

def regularDate(raw, id):
    if raw == None or raw == 'null':
        return None
    date = None
    miso = re.match(r'\d\d\d\d(-\d\d(-\d\d)?)?', raw)
    mday = re.match(fr'({mopat})\s+(\d\d?)\D.*?(\d\d\d\d)', raw)
    mmon = re.match(fr'({mopat}).*?(\d\d\d\d)', raw)
    if miso:
        date = miso.group()
    elif mday:
        (m, d, y) = mday.groups()
        month = str(momap.get(m, 0)).zfill(2)
        date = f'{y}-{month}-{d.zfill(2)}'
    elif mmon:
        (m, y) = mmon.groups()
        month = str(momap.get(m, 0)).zfill(2)
        date = f'{y}-{month}'        
    if id.startswith('sim_new-york-times_1887') and date < '1887-09' and id.find('_37_') >= 0:
        date = date.replace('1887', '1888')
    if id.startswith('sim_harpers-weekly_1885') and id.find('_30_') >= 0:
        date = date.replace('1885', '1886')
    return date

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='IA Periodical Metadata',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('idsPath', metavar='<path>', help='ids path')
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('IA Periodical Metadata').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')

    reg_date = udf(lambda s, id: regularDate(s, id))

    ids = spark.read.text(config.idsPath).toDF('issue')

    spark.read.json(config.inputPath
        ).na.drop(subset=['identifier', 'collection']
        ).select(col('identifier').alias('issue'),
                 (col('collection')[0]).alias('series'),
                 col('date').alias('rawdate'),
                 reg_date('date', 'identifier').alias('date')
        ).filter(col('series').startswith('pub_') | col('series').startswith('newspaperarchive-')
        ).distinct(
        ).join(ids, ['issue'], 'left_semi'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
