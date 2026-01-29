import argparse, glob, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (array, col, collect_list, explode, regexp_replace,
                                   sort_array, split, struct, udf)
import pyspark.sql.functions as f
from warcio.archiveiterator import ArchiveIterator
from io import StringIO, BytesIO
from lxml import etree
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
    batch: str
    alto: str
    text: str
    sourceFile: str
    width: int
    height: int
    dpi: int
    regions: list[Region]

ns = {'': 'http://www.w3.org/1999/xhtml'}

def parseHOCR(rid, content):
    (batchfile, fname) = rid
    res = []
    if content == None or content == '':
        return res
    try:
        tree = etree.parse(BytesIO(content))
        page = tree.find(".//div[@class='ocr_page']", namespaces=ns)
        text = ''
        regions = []
        width, height, dpi = 0, 0, 0
        m = re.search(r'bbox (\d+) (\d+) (\d+) (\d+)', page.attrib.get('title', ''))
        if m:
            (x1, y1, x2, y2) = map(int, m.groups())
            width, height = x2 - x1, y2 - y1
        for par in page.findall(".//p[@class='ocr_par']", namespaces=ns):
            for line in par.findall(".//span[@class='ocr_line']", namespaces=ns):
                start = len(text)
                for w in line.findall(".//span[@class='ocrx_word']", namespaces=ns):
                    text += w.text
                    if w.tail != None:
                        text += w.tail
                m = re.search(r'bbox (\d+) (\d+) (\d+) (\d+)',
                              line.attrib.get('title', ''))
                if m:
                    (x1, y1, x2, y2) = map(int, m.groups())
                    regions.append(Row(start=start, length=len(text)-start,
                                       coords=Row(x=x1, y=y1, w=x2-x1, h=y2-y1, b=y2-y1)))
                text += '\n'
            text += '\n'
        m = re.search(r'ppageno (\d+)', page.attrib.get('title', ''))
        if m:
            seq = int(m.group(1))
        else:
            seq = -1
        m = re.search(r'x_source ([^;\s"]+)', page.attrib.get('title', ''))
        if m:
            imfile = m.group(1)
        else:
            imfile = fname
        res.append(Row(fname=fname, text=text,
                       pages=[Row(id=imfile, seq=seq,
                                  width=width, height=height, dpi=dpi, regions=regions)]))
    except:
        print('# Error parsing ' + fname, file=sys.stderr)
        res = []

    return res

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
    parser = argparse.ArgumentParser(description='MDZ hOCR Import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('manifestPath', metavar='<manifest path>', help='manifest path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    
    paths = glob.glob(config.inputPath)

    meta = spark.read.json(config.manifestPath
               ).select((split('@id', '/')[6]).alias('book'), col('label').alias('title'),
                        f.posexplode(col('sequences')[0]['canvases']).alias('seq', 'info')
               ).withColumn('id', col('info.seeAlso.@id'))

    spark.sparkContext.parallelize(paths, len(paths)
        ).flatMap(lambda fname: warcFiles(fname)
        ).groupByKey(len(paths) * 10
        ).mapValues(lambda barr: b''.join(barr)
        ).flatMap(lambda r: parseHOCR(*r)
        ).toDF('struct<id: string, text: string, pages: array<struct<id: string, seq: int, width: int, height: int, dpi: int, regions: array<struct<start: int, length: int, coords: struct<x: int, y: int, w: int, h: int, b: int>>>>>>'
        ).join(meta, 'id'
        ).select('id', 'book', 'seq', 'title', 'text',
                 regexp_replace('info.label', r'\s*\(.*$', '').alias('pno'),
                 array(col('pages')[0].withField('id', col('info.images')[0]['resource']['@id']
                                     ).withField('iiif', col('info.images')[0]['resource']['service']['@id'])).alias('pages')
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
