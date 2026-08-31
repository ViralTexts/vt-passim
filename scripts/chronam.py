import argparse, glob, os, re, sys
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from warcio.archiveiterator import ArchiveIterator
from io import StringIO, BytesIO
from lxml import etree
from dataclasses import dataclass
import tarfile

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
    series: str
    date: str
    ed: str
    seq: int
    text: str
    sourceFile: str
    width: int
    height: int
    dpi: int
    regions: list[Region]

def parseAlto(rid, content):
    (batchfile, fname) = rid
    batch = sub(r'\.tar.bz2$', '', os.path.basename(batchfile))
    # if content.startswith('\ufeff'):
    #     content = content[1:]

    try:
        tree = etree.parse(BytesIO(content))
        (series, year, month, day, ed, seq, rest) = fname.split('/', 7)
        series = '/lccn/' + series
        date = '-'.join((year, month, day))
        ed = ed.replace('ed-', '')
        nseq = int(seq.replace('seq-', ''))
    except:
        return AltoRec(batch, fname, '', '', '', 0, 'fubar', '', 0, 0, 0, [])
    root = tree.find('.')
    ns = root.nsmap
    try:
        sourceFile = tree.findtext('./Description/sourceImageInformation/fileName',
                                   namespaces=ns).strip()
    except:
        sourceFile = ''
    try:
        layout = tree.find('./Layout/Page', namespaces=ns)
        width = int(layout.get('WIDTH'))
        height = int(layout.get('HEIGHT'))
    except:
        width, height = 0, 0
    dpi = 0
    text = ''
    regions = []
    for block in tree.findall('.//TextBlock', namespaces=ns):
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
    return AltoRec(batch, fname, series, date, ed, nseq,
                   text, sourceFile, width, height, dpi, regions)

def tarFiles(path):
    tar = tarfile.open(path, 'r')
    for tarinfo in tar:
        if tarinfo.isfile() and tarinfo.name.endswith('.xml'):
            raw = tar.extractfile(tarinfo).read()
            yield ((path, tarinfo.name), raw)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NDNP Alto Import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('NDNP Alto Import').getOrCreate()

    paths = glob.glob(config.inputPath)

    spark.sparkContext.parallelize(paths, len(paths)
        ).flatMap(lambda fname: tarFiles(fname)
        ).groupByKey(len(paths) * 10
        ).mapValues(lambda barr: b''.join(barr)
        ).map(lambda r: parseAlto(*r)
        ).toDF(
        ).filter(col('text') != 'fubar'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
