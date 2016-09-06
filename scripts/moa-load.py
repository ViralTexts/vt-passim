from __future__ import print_function

import sys
from dateutil import parser
from datetime import *

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, regexp_replace, split, udf

def normdate(s):
    try:
        return parser.parse(s, default=datetime(1800,1,1), fuzzy=True).date().isoformat()
    except ValueError:
        return None

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: moa-load.py <input xml> <series json> <output parquet>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MoA Load")
    sqlContext = SQLContext(sc)

    raw = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='doc').load(sys.argv[1])

    series = sqlContext.read.json(sys.argv[2])

    convdate = udf(lambda s: normdate(s))
    mkurl = udf(lambda ser, id: 'http://ebooks.library.cornell.edu/cgi/t/text/text-idx?c=%s;idno=%s' % (ser, id))

    df = raw.select((split(col('docno'), '_')[0]).alias('moaseries'),
                    (split(col('docno'), '_')[1]).alias('id'),
                    convdate(col('date')).alias('date'),
                    regexp_replace(regexp_replace(col('text'), '&', '&amp;'),
                                   '<', '&lt;').alias('text'))\
            .withColumn('issue', col('id'))\
            .withColumn('url', mkurl(col('moaseries'), col('id')))

    df.join(series, (df.moaseries == series.moaseries) \
            & (df.date >= series.startdate) & (df.date <= series.enddate))\
        .drop('moaseries').drop('startdate').drop('enddate')\
        .write.save(sys.argv[3])
    
    sc.stop()
