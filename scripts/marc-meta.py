import argparse
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import (coalesce, col, concat, explode, lit, size, udf, struct, length, substring)
from pyspark.sql.functions import regexp_replace as rr
import pyspark.sql.functions as f

def subfield1(data, tag, code):
    fields = [d.subfield for d in data if d._tag == tag]
    res = None
    if fields and len(fields) > 0:
        subs = [s._VALUE for s in fields[0] if s._code == code]
        if subs:
            res = subs[0].strip(':;, ')
    return res

def isubfield1(data, tag, ind1, code):
    fields = [d.subfield for d in data if d._tag == tag and d._ind1 == ind1]
    res = None
    if fields and len(fields) > 0:
        subs = [s._VALUE for s in fields[0] if s._code == code]
        if subs:
            res = subs[0].strip(':;, ')
    return res

def i2subfield1(data, tag, ind2, code):
    fields = [d.subfield for d in data if d._tag == tag and d._ind2 == ind2]
    res = None
    if fields and len(fields) > 0:
        subs = [s._VALUE for s in fields[0] if s._code == code]
        if subs:
            res = subs[0].strip(':;, ')
    return res

def field1(data, tag):
    fields = [d.subfield for d in data if (d._tag == tag and d.subfield != None)]
    res = None
    if fields and len(fields) > 0:
        res = ' '.join([s._VALUE for s in fields[0]])
    return res

def getLink(subfield):
    fields = [s['_VALUE'] for s in subfield
              if s['_code'] == 'w' and s['_VALUE'].startswith('(DLC)')]
    res = None
    if fields and len(fields) > 0:
        res = '/lccn/' + fields[0].replace('(DLC)', '').replace(' ', '')
    return res

def getRelations(data):
    fields = [(d._tag, d['_ind2'], getLink(d.subfield)) for d in data
              if d._tag in [775, 780, 785]]
    res = None
    if fields and len(fields) > 0:
        res = fields
    return res

def getPlace(subfield):
    r = {s._code: s._VALUE.strip(':;,. ') for s in subfield}
    return (r.get('a'), r.get('b'), r.get('c'), r.get('d'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract MARC fields',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Extract MARC fields').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    raw = spark.read.load(config.inputPath)

    get_subfield1 = udf(lambda d, tag, code: subfield1(d, tag, code))
    get_isubfield1 = udf(lambda d, tag, ind1, code: isubfield1(d, tag, ind1, code))
    get_i2subfield1 = udf(lambda d, tag, ind2, code: i2subfield1(d, tag, ind2, code))
    get_field1 = udf(lambda d, tag: field1(d, tag))
    norm_link = udf(lambda u: u.replace('http://chroniclingamerica.loc.gov/', 'http://www.loc.gov/chroniclingamerica/').replace('http://loc.gov/', 'http://www.loc.gov/').rstrip('/ ') if u else None)

    ## Could be useful to get extra provenance information from:
    ## get_i2subfield1('datafield', lit(651), lit('7'), lit('a'))),

    flat = raw.select(concat(lit('/lccn/'),
                             f.translate(get_subfield1('datafield', lit(10), lit('a')),
                                         ' ', '')).alias('series'),
                      get_field1('datafield', lit(245)).alias('title'),
                      get_field1('datafield', lit(246)).alias('alttitle'),
                      get_subfield1('datafield', lit(250), lit('a')).alias('edition'),
                      coalesce(get_subfield1('datafield', lit(264), lit('a')),
                               get_subfield1('datafield', lit(260), lit('a'))
                               ).alias('placeOfPublication'),
                      coalesce(get_subfield1('datafield', lit(264), lit('b')),
                               get_subfield1('datafield', lit(260), lit('b'))
                               ).alias('publisher'),
                      # get_field1('datafield', lit(310)).alias('frequencyStmt'),
                      # get_field1('datafield', lit(321)).alias('formerFrequency'),
                      get_subfield1('datafield', lit(338), lit('a')).alias('carrier'),
                      # get_field1('datafield', lit(43)).alias('geocode'),
                      get_isubfield1('datafield', lit(362), lit('0'), lit('a')).alias('dateseq'),
                      get_isubfield1('datafield', lit(362), lit('1'), lit('a')).alias('udateseq'),
                      rr(get_subfield1('datafield', lit(752), lit('a')), '\.$', '').alias('country'),
                      rr(get_subfield1('datafield', lit(752), lit('b')), '\.$', '').alias('div1'),
                      rr(get_subfield1('datafield', lit(752), lit('c')), '\.$', '').alias('div2'),
                      rr(get_subfield1('datafield', lit(752), lit('d')), '\.$', '').alias('city'),
                      rr(rr(rr(get_subfield1('datafield', lit(856), lit('u')),
                               'https?://chroniclingamerica.loc.gov/',
                               'http://www.loc.gov/chroniclingamerica/'),
                            'https?://loc.gov/', 'http://www.loc.gov'),
                         '/$', '').alias('related'),
                      # (f.filter('datafield', lambda c: c['_tag'] == 856)[0]['_ind2']).alias('rind2'),
                      (f.filter('controlfield', lambda c: c['_tag'] == 8)[0]['_VALUE']).alias('marc8')
                    ).withColumn('startDate', substring('marc8', 8, 4)
                    ).withColumn('endDate', substring('marc8', 12, 4)
                    ).withColumn('placeCode', substring('marc8', 16, 3)
                    ).withColumn('frequency', substring('marc8', 19, 1)
                    ).withColumn('regularity', substring('marc8', 20, 1)
                    ).withColumn('lang', substring('marc8', 36, 3)
                    ).drop('marc8'
                    )

    get_relations = udf(lambda d: getRelations(d),
                        'array<struct<type: int, subtype: string, link: string>>')

    links = raw.select(concat(lit('/lccn/'),
                              f.translate(get_subfield1('datafield', lit(10), lit('a')),
                                          ' ', '')).alias('series'),
                       explode(get_relations('datafield')).alias('link')
                ).select('series', 'link.*'
                ).filter(col('link').isNotNull())

    get_place = udf(lambda d: getPlace(d),
                     'struct<country: string, div1: string, div2: string, city: string>')

    raw.select(concat(lit('/lccn/'),
                      f.translate(get_subfield1('datafield', lit(10), lit('a')),
                                  ' ', '')).alias('series'),
               explode(f.filter('datafield', lambda c: c['_tag'] == 752)).alias('place')
        ).select('series', get_place(col('place')['subfield']).alias('place')
        ).select('series', 'place.*'
        ).sort('series', 'country', 'div1', 'city'
        ).coalesce(1
        ).write.csv(config.outputPath + '/places', header=True, escape='"', mode='overwrite')

    # flat.write.json(config.outputPath)
    flat.sort('series').coalesce(1).write.csv(config.outputPath + '/meta',
                                              header=True, escape='"', mode='overwrite')

    links.sort('series').coalesce(1).write.csv(config.outputPath + '/links',
                                               header=True, escape='"', mode='overwrite')

    spark.stop()
    
