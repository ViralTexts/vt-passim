import argparse, glob, json, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, col, collect_list, explode, lit, sort_array, struct, udf
import pyspark.sql.functions as f
from warcio.archiveiterator import ArchiveIterator
from dataclasses import dataclass

@dataclass
class Coords:
    x: int
    y: int
    w: int
    h: int
    b: int

@dataclass
class Region:
    start: int
    length: int
    coords: Coords

@dataclass
class AltoRec:
    ocr: str
    text: str
    regions: list[Region]

def parseAnnotations(content):
    try:
        rec = json.loads(content)
    except:
        return AltoRec(None, None, None)

    fname = rec['@id']

    text = ''
    regions = []
    lastx = 0
    lasty = 0
    for wrec in rec['resources']:
        cs = wrec['on'].split('#xywh=', 2)[1]
        (x, y, w, h) = cs.split(',')
        x = int(x)
        y = int(y)
        w = int(w)
        h = int(h)
        if x > lastx and abs(y - lasty) < 50:
            text += ' '
        elif len(text) > 0:
            text += '\n'
        start = len(text)
        text += wrec['resource'].get('chars', '')
        regions.append(Region(start, len(text) - start,
                              Coords(x, y, w, h, h)))
        lastx = x
        lasty = y

    return AltoRec(fname, text, regions)

def warcFiles(path):
    with open(path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                file = record.rec_headers.get_header('WARC-Target-URI')
                # print(file)
                code = record.http_headers.get_statuscode()
                if code != '200' and code != '206':
                    continue

                raw = record.content_stream().read()
                yield ((path, file), raw)  #.decode())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Texs DNP OCR IIIF Annotation Import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('metsPath', metavar='<mets path>', help='mets path')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    print(config.inputPath)
    paths = glob.glob(config.inputPath)
    print(paths)

    mets = spark.read.json(config.metsPath, recursiveFileLookup='true', pathGlobFilter='*.json.gz'
                ).filter(col('series').isNotNull() & (col('date') < "1923"))

    parse_annotations = udf(lambda v: parseAnnotations(v),
                            'struct<ocr: string, text: string, regions: array<struct<start: int, length: int, coords: struct<x: int, y: int, w: int, h: int, b: int>>>>')

    spark.sparkContext.parallelize(paths, len(paths)
        ).flatMap(lambda fname: warcFiles(fname)
        ).groupByKey(len(paths) * 10
        ).mapValues(lambda barr: (b''.join(barr)).decode()
        ).toDF(
        ).withColumn('info', parse_annotations('_2')
        ).select('info.*'
        ).join(mets, ['ocr'], 'right_outer'
        ).withColumn('pp', col('pp').cast('int')
        ).withColumn('seq', col('seq').cast('int')
        ).withColumn('pages', array(struct(col('image').alias('id'),
                                           col('iiif'),
                                           col('page_access').alias('viewer'),
                                           col('seq'),
                                           col('width').cast('int').alias('width'),
                                           col('height').cast('int').alias('height'),
                                           lit(0).cast('int').alias('dpi'),
                                           col('regions')))
        ).drop('ocr', 'image', 'iiif', 'width', 'height', 'regions', 'page_access'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
