import argparse, json, os, re, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from kraken.lib import xml

def pageRecord(path):
    path = re.sub(r'^file:/*/', '/', path)
    doc = xml.XMLPage(path)
    text = ''
    start = 0
    regions = []
    for line in doc.get_sorted_lines(ro='line_implicit'):
        x1 = min(x[0] for x in line.boundary)
        x2 = max(x[0] for x in line.boundary)
        y1 = min(x[1] for x in line.boundary)
        y2 = max(x[1] for x in line.boundary)
        ltext = line.text.strip()
        text += ltext + '\n'
        regions.append(Row(start=start, length=len(ltext),
                           coords=Row(x=x1, y=y1, w=x2-x1, h=y2-y1, b=y2-y1)))
        start += len(ltext) + 1
    width, height = doc.image_size
    return Row(id=path, text=text, pages=[Row(id=str(doc.imagename), seq=0, width=width,
                                              height=height, dpi=0, regions=regions)])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PageXML import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    spark.read.load(config.inputPath, format='text', wholetext='true', recursiveFileLookup='true',
                    pathGlobFilter='*.xml'
        ).select(f.input_file_name().alias('path')
        ).rdd.map(lambda r: pageRecord(r.path)
        ).toDF('struct<id: string, text: string, pages: array<struct<id: string, seq: int, width: int, height: int, dpi: int, regions: array<struct<start: int, length: int, coords: struct<x: int, y: int, w: int, h: int, b: int>>>>>>'
        ).write.json(config.outputPath, mode='overwrite')
    
    spark.stop()
