from __future__ import print_function

import sys, os
from re import sub
import zipfile, json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StringType

def getSeries(fname):
    with zipfile.ZipFile(fname, 'r') as zf:
        names = zf.namelist()
        mfile = [f for f in names if f.endswith('.metadata.json')]
        series = fname
        if len(mfile) > 0:
            m = json.loads(zf.read(mfile[0]))
            series = m['identifier'][0]
        for f in filter(lambda x: x.endswith('.fulltext.json'), names):
            r = json.loads(zf.read(f))
            text = "\n".join(r['contentAsText']).replace('<', '&lt;')
            yield Row(id=r['identifier'][0], series=series,
                      date=r['date'][0], lang=r['language'][0], text=text)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: europeana.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Europeana Import")
    sqlContext = SQLContext(sc)

    x = [os.path.join(d[0], f) for d in os.walk(sys.argv[1]) for f in d[2] if f.endswith('zip')]
    sc.parallelize(x).flatMap(getSeries).toDF().write.save(sys.argv[2])

    sc.stop()
