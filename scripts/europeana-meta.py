from __future__ import print_function

import sys, os
from re import sub
import zipfile, json

# from pyspark import SparkContext
# from pyspark.sql import SQLContext
# from pyspark.sql import Row
# from pyspark.sql.types import StringType

def getSeries(fname):
    with zipfile.ZipFile(fname, 'r') as zf:
        names = zf.namelist()
        mfile = [f for f in names if f.endswith('.metadata.json')]
        series = 'europeana/' + sub('^.*newspapers-by-country/', '',
                                    sub('[\x80-\xff]', '', fname).replace('.zip', ''))
        if len(mfile) > 0:
            m = json.loads(zf.read(mfile[0]))
            return {'series': series, 'title': m['title'][0], 'lang': m['language']}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: europeana.py <input> <output>", file=sys.stderr)
        exit(-1)
    # sc = SparkContext(appName="Europeana Import")
    # sqlContext = SQLContext(sc)

    x = [os.path.join(d[0], f) for d in os.walk(sys.argv[1]) for f in d[2] if f.endswith('zip')]
    for f in x:
        print(json.dumps(getSeries(f)))
        
    # sc.parallelize(x, 200).flatMap(getSeries).toDF().write.save(sys.argv[2])

    # sc.stop()
