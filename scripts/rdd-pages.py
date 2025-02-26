import argparse, os
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree
from dataclasses import dataclass

ns = {None: 'http://www.tei-c.org/ns/1.0'}

def cleanText(s):
    return sub(r'\n[ ]+', '\n', sub(r'[ \t]+', ' ', s))

class BookStream(object):
    lines = set(['head', 'p', 'row'])
    spacers = set(['forename', 'roleName', 'cell', 'salute'])
    def __init__(self, book):
        self.book = book
        self.seq = 1
        self.date = None
        self.print = False
        self.buf = ''
        self.res = []

    def start(self, elem, attrib):
        tag = etree.QName(elem).localname
        if not self.print and tag == 'date' and 'when' in attrib and len(attrib['when']) == 10:
            self.date = attrib['when']
        if tag == 'pb' and self.print:
            if self.buf != '':
                self.res.append(Row(id=self.book+'_'+str(self.seq), date=self.date,
                                    seq=self.seq, text=cleanText(self.buf)))
                self.seq += 1
                self.buf = ''
        elif tag == 'lb' and self.print:
            self.buf += '\n'
            
    def end(self, elem):
        tag = etree.QName(elem).localname
        if tag == 'teiHeader':
            self.print = True
        elif (tag in self.lines) and self.print:
            self.buf += '\n'
        elif (tag in self.spacers) and self.print:
            self.buf += ' '

    def data(self, data):
        if self.print:
            self.buf += data

    def comment(self, text):
        pass

    def close(self):
        if self.buf != '':
            self.res.append(Row(id=self.book+'_'+str(self.seq), date=self.date,
                                seq=self.seq, text=cleanText(self.buf)))
        res = self.res
        self.buf = ''
        self.print = False
        self.res = []
        self.seq = 1
        self.date = None
        return res

def parseFile(path, content):
    book = sub(r'(.TEI-P5)?.xml$', '', os.path.basename(path))

    parser = etree.XMLParser(target = BookStream(book))
    result = etree.parse(BytesIO(content), parser)
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DTA Pages',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('DTA Pages').getOrCreate()

    spark.read.load(config.inputPath, format='binaryFile', recursiveFileLookup='true',
                    pathGlobFilter='*.xml'
        ).rdd.flatMap(lambda r: parseFile(r.path, r.content)
        ).toDF(
        ).withColumn('seq', col('seq').cast('int')
        ).write.parquet(config.outputPath, mode='overwrite')

    spark.stop()

                    
