from __future__ import print_function

import sys, os, json, zipfile
from re import sub

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

def getSeries(fname):
    with zipfile.ZipFile(fname, 'r') as zf:
        names = zf.namelist()
        mfile = [f for f in names if f.endswith('.metadata.json')]
        series = 'europeana/' + sub('^.*newspapers-by-country/', '',
                                    sub('[\x80-\xff]', '', fname).replace('.zip', ''))
        if len(mfile) > 0:
            m = json.loads(zf.read(mfile[0]))
        for f in names:
            if f.endswith('.fulltext.json'):
                r = json.loads(zf.read(f))
                if r.has_key('contentAsText') and r.has_key('identifier'):
                    issue = 'europeana/' + sub('^.*/([^/]+)$', '\\1', r['identifier'][0])
                    date = r['date'][0]
                    lang = r['language'][0]
                    seq = 0
                    for page in r['contentAsText']:
                        seq = seq + 1
                        yield Row(id=issue + '/' + str(seq), issue=issue, series=series, seq=seq,
                                  date=date, lang=lang,
                                  text=page)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: europeana.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Europeana import').getOrCreate()

    x = [os.path.join(d[0], f) for d in os.walk(sys.argv[1]) for f in d[2] if f.endswith('zip')]
    spark.sparkContext.parallelize(x, 200)\
      .flatMap(getSeries).toDF()\
      .withColumn('seq', col('seq').cast('int'))\
      .repartition(100)\
      .write.save(sys.argv[2])
      # pyspark type inference makes int into long, so cast to int

    spark.stop()
