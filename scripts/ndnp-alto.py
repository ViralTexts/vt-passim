import argparse, glob, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
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

def parseAlto(batchfile, fname, content):
    batch = sub(r'^.*/batch_', '', sub(r'\.warc\.gz$', '', batchfile))
    # if content.startswith('\ufeff'):
    #     content = content[1:]

    tree = etree.parse(BytesIO(content))
    root = tree.find('.')
    ns = root.nsmap
    try:
        sourceFile = tree.findtext('/Description/sourceImageInformation/fileName',
                                   namespaces=ns).strip()
    except:
        sourceFile = ''
    try:
        layout = tree.find('/Layout/Page', namespaces=ns)
        width = int(layout.get('WIDTH'))
        height = int(layout.get('HEIGHT'))
    except:
        width, height = 0, 0
    dpi = 0
    text = ''
    regions = []
    for block in tree.findall('//TextBlock', namespaces=ns):
        for line in block.findall('./TextLine', namespaces=ns):
            tok = 0
            for e in line.iter():
                tag = e.tag
                if tag.endswith('}String'):
                    if tok > 0:
                        text += ' '
                    tok += 1
                    start = len(text)
                    text += e.get('CONTENT')
                    try:
                        regions.append(Region(start, len(text) - start,
                                              Coords(int(float(e.get('HPOS'))),
                                                     int(float(e.get('VPOS'))),
                                                     int(float(e.get('WIDTH'))),
                                                     int(float(e.get('HEIGHT'))),
                                                     int(float(e.get('HEIGHT'))))))
                    except:
                        1
                elif tag.endswith('}HYP'):
                    text += '\u00ad'
            text += '\n'
        text += '\n'
    return AltoRec(batch, fname, text, sourceFile, width, height, dpi, regions)

def warcFiles(path):
    with open(path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                file = record.rec_headers.get_header('WARC-Target-URI')
                # print(file)
                if record.http_headers.get_statuscode() != '200':
                    continue

                raw = record.content_stream().read()
                yield (path, file, raw)  #.decode())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NDNP Alto Import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('NDNP Alto Import').getOrCreate()

    paths = glob.glob(config.inputPath)

    spark.sparkContext.parallelize(paths, len(paths)
        ).flatMap(lambda fname: warcFiles(fname)
        ).map(lambda r: parseAlto(*r)
        ).toDF(
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
