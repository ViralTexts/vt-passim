import argparse, os, re, regex, sys
from unicodedata import normalize
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (array_join, col, collect_list, element_at, regexp_replace,
                                   sort_array, split, struct, udf)
import pyspark.sql.functions as f

def textLocs(text, urn):
    res = []
    buf = ''
    pos = 0
    cur = None
    for line in text.split('\n'):
        if re.match(r'^<column', line):
            if (pos > 0) and ('ιιι' not in buf):
                buf = sub(r'^\s+', '', sub(r'\n\n+', '\n\n', buf)) + '\n'
                res.append((buf, urn + ':' + str(pos)))
            cur = sub(r"^<column\s+n\s*=\s*'(\d+)'.*$", '\\1', line)
            pos += 1
            buf = ''
            continue
        line = sub(r'</?[A-Za-z][^>]*>', '', line)
        if len(line) > 0 and len(regex.findall(r'\p{InGreek}', line))/len(line) > 0.5:
            buf += sub(r'\s+$', '', line)
        buf += '\n'
    if (pos > 0) and ('ιιι' not in buf):
        buf = sub(r'^\s+', '', sub(r'\n\n+', '\n\n', buf)) + '\n'
        res.append((buf, urn + ':' + str(pos)))
    return res

def makeLocs(info):
    off = 0
    res = []
    for chunk in info:
        res.append((chunk[1], off, len(chunk[0])))
        off += len(chunk[0])
    return res

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Grab IDI Greek text',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('metaPath', metavar='<meta path>', help='meta path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()
    nfd_norm = udf(lambda s: normalize('NFD', s))
    bookno = udf(lambda fname: sub(r'\.xml$', '', os.path.basename(fname)))

    text_locs = udf(lambda s, urn: textLocs(s, urn), 'array<struct<text: string, loc:string>>')
    make_locs = udf(lambda info: makeLocs(info),
                    'array<struct<loc: string, start: int, length: int>>')

    meta = spark.read.csv(config.metaPath, header=True, escape='"'
               ).select('book',
                        f.format_string('urn:cts:greekLit:pg%s.pg-grc1', 'prnc').alias('id'),
                        f.format_string('urn:cts:greekLit:pg%s', 'prnc').alias('urn'),
                        col('volume dates').alias('date'))

    spark.read.text(config.inputPath, recursiveFileLookup=True, wholetext=True,
                    pathGlobFilter='*.xml'
        ).select(bookno(f.input_file_name()).alias('book'),
                 nfd_norm('value').alias('text')
        ).join(meta, 'book'
        ).withColumn('info', text_locs('text', 'urn')
        ).select('id', col('id').alias('book'), 'date',
                 array_join(col('info')['text'], '').alias('text'),
                 make_locs('info').alias('locs')
        ).write.save(config.outputPath, mode='overwrite')
    
    spark.stop()
