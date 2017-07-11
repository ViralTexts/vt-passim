from __future__ import print_function
from re import sub
import sys
from os.path import basename, splitext
from pyspark.sql import SparkSession

def parseEd(f):
    res = dict()
    inMeta = False
    text = ''
    for line in f[1].split('\n'):
        if line == '---':
            if inMeta:
                inMeta = False
            else:
                inMeta = True
        elif inMeta:
            (k, s, v) = line.partition(':')
            if k != '':
                res[k] = v.strip()
        else:
            text += sub('</?[A-Za-z][^>]*>', '', sub('^\-*\s*(\{[^\}]*\})?\s*', '', line))
            text += '\n'
    res['text'] = sub('\n{3,}', '\n\n', sub('\s*<br>\s*$', '', text)).strip() + '\n'
    res['id'] = (splitext(basename(f[0])))[0]
    return res

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ed-load.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Load Ed Markdown').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.sparkContext._jsc.hadoopConfiguration()\
                           .set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')
    spark.sparkContext.wholeTextFiles(sys.argv[1])\
        .filter(lambda f: f[0].endswith('.md'))\
        .map(lambda f: parseEd(f))\
        .toDF()\
        .write.save(sys.argv[2])
    spark.stop()
