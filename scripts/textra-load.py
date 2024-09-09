import argparse, json, os, re, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from kraken.lib import xml

def pageRecord(path, content):
    path = re.sub(r'^file:/*/', '/', path)
    rec = json.loads(content)
    text = ''
    start = 0
    regions = []
    for line in rec['observations']:
        c = line['observation']['bounds']
        ltext = line['observation']['text'].strip()
        text += ltext + '\n'
        regions.append(Row(start=start, length=len(ltext),
                           coords=Row(x=c['x1'], y=c['y2'],
                                      w=c['x2']-c['x1'], h=c['y1']-c['y2'], b=c['y1']-c['y2'])))
        start += len(ltext) + 1
    return Row(id=path, text=text,
               pages=[Row(id=path, seq=0, width=1, height=1, dpi=0, regions=regions)])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Textra import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    spark.read.load(config.inputPath, format='text', wholetext='true', recursiveFileLookup='true',
                    pathGlobFilter='*.json'
        ).withColumn('path', f.input_file_name()
        ).rdd.map(lambda r: pageRecord(r.path, r.value)
        ).toDF(#'struct<id: string, text: string, pages: array<struct<id: string, seq: int, width: int, height: int, dpi: int, regions: array<struct<start: int, length: int, coords: struct<x: int, y: int, w: int, h: int, b: int>>>>>>'
        ).write.json(config.outputPath, mode='overwrite')
    
    spark.stop()
