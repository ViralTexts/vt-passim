import argparse, os, re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree

ns = {'': 'http://www.w3.org/1999/xhtml'}

def parseHOCR(path, content):
    res = []
    if content != None and content != '':
        dir = os.path.basename(os.path.dirname(path))
        file = re.sub(r'_hocr.html$', '', os.path.basename(path))
        if dir != file:
            book = dir + '/' + file
        else:
            book = file
        tree = etree.parse(BytesIO(content))
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
                    for w in line.findall(".//span[@class='ocrx_word']", namespaces=ns):
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
            res.append(Row(id=book + ('_%04d' % uid), book=book, seq=seq, text=text,
                           pages=[Row(id=book + ('_%04d' % seq), seq=seq,
                                      width=width, height=height, dpi=dpi, regions=regions)]))
            uid += 1
            
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

    spark.read.load(config.inputPath, format='binaryFile', pathGlobFilter='*.html',
                    recursiveFileLookup='true'
        ).rdd.flatMap(lambda r: parseHOCR(r.path, r.content)
        ).toDF(
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
