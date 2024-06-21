import argparse, os, re, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree

ns = {'': 'http://www.w3.org/1999/xhtml'}

def parseHOCR(path, content):
    res = []
    if content == None or content == '':
        return res
    dir = os.path.basename(os.path.dirname(path))
    file = re.sub(r'_hocr.html(.gz)?$', '', os.path.basename(path))
    if dir != file:
        book = dir + '/' + file
    else:
        book = file
    try:
        tree = etree.parse(BytesIO(content.encode()))
        uid = 0
        for page in tree.findall("//div[@class='ocr_page']", namespaces=ns):
            text = ''
            regions = []
            width, height, dpi = 0, 0, 0
            m = re.search(r'bbox (\d+) (\d+) (\d+) (\d+).*scan_res (\d+)',
                          page.attrib.get('title', ''))
            if m:
                (x1, y1, x2, y2, dpi) = map(int, m.groups())
                width, height = x2 - x1, y2 - y1
            for par in page.findall(".//p[@class='ocr_par']", namespaces=ns):
                for line in par.findall(".//span[@class='ocr_line']", namespaces=ns):
                    start = len(text)
                    line_start = True
                    for w in line.findall(".//span[@class='ocrx_word']", namespaces=ns):
                        if line_start:
                            line_start = False
                        else:
                            text += ' '
                        text += w.findtext('.', namespaces=ns)
                    m = re.search(r'bbox (\d+) (\d+) (\d+) (\d+)',
                                  line.attrib.get('title', ''))
                    if m:
                        (x1, y1, x2, y2) = map(int, m.groups())
                        regions.append(Row(start=start, length=len(text)-start,
                                           coords=Row(x=x1, y=y1, w=x2-x1, h=y2-y1, b=y2-y1)))
                    text += '\n'
                text += '\n'
            seq = int(re.sub(r'^.*_0*(\d+)', r'\1', page.attrib.get('id', '')))
            m = re.search(r'image "(?:/tmp/)?([^"]+)"', page.attrib.get('title', ''))
            if m:
                imfile = '/'.join([dir, file + '_jp2.zip', m.group(1).replace('/', '%2F')])
            else:
                imfile = book + ('_%04d' % seq)
            res.append(Row(id=book + ('_%04d' % uid), book=book, seq=seq, text=text,
                           pages=[Row(id=imfile, seq=seq,
                                      width=width, height=height, dpi=dpi, regions=regions)]))
            uid += 1
    except:
        print('# Error parsing ' + path, file=sys.stderr)
        res = []

    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='IA hOCR import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    wd = os.getcwd()
    wd = wd if wd.startswith('file:') else 'file:' + wd

    spark = SparkSession.builder.appName('IA hOCR import').getOrCreate()

    spark.read.load(config.inputPath, format='text', wholetext='true', recursiveFileLookup='true',
        ).withColumn('path', f.input_file_name()
        ).rdd.flatMap(lambda r: parseHOCR(r.path, r.value)
        ).toDF('struct<id: string, book: string, seq: int, text: string, pages: array<struct<id: string, seq: int, width: int, height: int, dpi: int, regions: array<struct<start: int, length: int, coords: struct<x: int, y: int, w: int, h: int, b: int>>>>>>'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
