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
class Rec:
    id: str
    issue: str
    series: str
    ed: str
    seq: int
    date: str
    batch: str
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
    if fname.startswith('http'):
        m = re.search(r'([^/]+)/(\d{4}-\d\d-\d\d)/([^/]+)/([^/]+)', fname)
        if m:
            (sn, date, ed, seq) = m.groups()
        else:
            return None

    series = '/lccn/' + sn
    issue = '/'.join([series, date, ed])
    id = '/'.join(['/ca', batch, sn, date, ed, seq])
    try:
        seq = int(sub(r'^seq-', '', seq))
    except:
        seq = 0

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
            for e in line.iter():
                tag = e.tag
                if tag.endswith('}String'):
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
                elif tag.endswith('}SP'):
                    text += ' '
                elif tag.endswith('}HYP'):
                    text += '\u00ad'
            text += '\n'
        text += '\n'
    return Rec(id, issue, series, sub(r'^ed-', '', ed), seq, date, batch,
               text, sourceFile, width, height, dpi, regions)

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
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
