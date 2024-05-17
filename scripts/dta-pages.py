import argparse, os, re
from re import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree
from dataclasses import dataclass

ns = {None: 'http://www.tei-c.org/ns/1.0'}

@dataclass
class RenditionSpan:
    rendition: str
    start: int
    length: int

@dataclass
class ZoneInfo:
    ztype: str
    place: str

@dataclass
class ZoneContent:
    info: ZoneInfo
    data: str
    rend: list

@dataclass
class Rec:
    id: str
    book: str
    seq: int
    page: str
    ztype: str
    place: str
    text: str
    rendition: list

class BookStream(object):
    lines = set(['head'])
    floats = set(['figure', 'note', 'table'])
    def __init__(self, book):
        self.book = book
        self.seq = -1
        self.pageID = ''
        self.zones = []
        self.rends = []
        self.res = []
    def start(self, elem, attrib):
        tag = etree.QName(elem).localname
        if tag == 'hi' and len(self.zones) > 0:
            self.rends.append(RenditionSpan(attrib.get('rendition', ''),
                                            len(self.zones[-1].data), 0))
        elif tag == 'fw' and len(self.zones) > 0:
            self.zones.append(ZoneContent(ZoneInfo(attrib.get('type', 'fw'),
                                                   attrib.get('place', 'fw')), '', []))
        elif tag == 'gap' and len(self.zones) > 0:
            self.zones.append(ZoneContent(ZoneInfo('quash', 'quash'), '', []))
        elif tag == 'g' and len(self.zones) > 0:
            if attrib.get('ref', '') == 'char:EOLhyphen':
                self.zones[-1].data += '\u00ad\n'
        elif tag == 'pb':
            # Record and restart all open rendition spans
            for i in range(len(self.rends)):
                r = self.rends[i]
                self.zones[-1].rend.append(RenditionSpan(r.rendition, r.start,
                                                         len(self.zones[-1].data) - r.start))
                self.rends[i] = RenditionSpan(r.rendition, 0, 0)
            # Remember and output all open zones
            if len(self.zones) > 0:
                zinfo = [r.info for r in reversed(self.zones)]
                while len(self.zones) > 0:
                    self.seq += 1
                    top = self.zones.pop()
                    self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                        seq=self.seq, page=self.pageID,
                                        ztype=top.info.ztype, place=top.info.place,
                                        text=top.data, rendition=top.rend))
            else:
                zinfo = [ZoneInfo('body', 'body')]
            for z in zinfo:
                self.zones.append(ZoneContent(z, '', []))
                # Output printed page number (n attribute not in brackets) here
                self.pageID = self.book + attrib.get('facs', '')
                pno = re.sub(r'\[[^\]]+\]', '', attrib.get('n', ''))
            if pno != '':
                self.seq += 1
                self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                    seq=self.seq, page=self.pageID,
                                    ztype='pageNum', place='pageNum', text=pno, rendition=[]))
        elif tag =='cb':
            # Record and restart all open rendition spans
            for i in range(len(self.rends)):
                r = self.rends[i]
                self.zones[-1].rend.append(RenditionSpan(r.rendition, r.start,
                                                         len(self.zones[-1].data) - r.start))
                self.rends[i] = RenditionSpan(r.rendition, 0, 0)
            # Remember and output all open zones
            if len(self.zones) > 0:
                zinfo = [r.info for r in reversed(self.zones)]
                while len(self.zones) > 0:
                    top = self.zones.pop()
                    if len(top.data) > 0:
                        self.seq += 1
                        self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                            seq=self.seq, page=self.pageID,
                                            ztype=top.info.ztype, place=top.info.place,
                                            text=top.data, rendition=top.rend))
            else:
                zinfo = [ZoneInfo('body', 'body')]
            for z in zinfo:
                self.zones.append(ZoneContent(z, '', []))
            # Output printed column number (n attribute not in brackets) here
            cno = re.sub(r'\[[^\]]+\]', '', attrib.get('n', ''))
            if cno != '':
                self.seq += 1
                self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                    seq=self.seq, page=self.pageID,
                                    ztype='colNum', place='colNum', text=cno, rendition=[]))
        elif tag in self.lines and len(self.zones) > 0:
            self.rends.append(RenditionSpan(tag, len(self.zones[-1].data), 0))
        elif tag in self.floats and len(self.zones) > 0:
            self.zones[-1].rend.append(RenditionSpan(tag, len(self.zones[-1].data), 0))
            self.zones.append(ZoneContent(ZoneInfo(tag, attrib.get('place', tag)), '', []))
            
    def end(self, elem):
        tag = etree.QName(elem).localname
        if tag == 'hi' and len(self.zones) > 0:
            start = self.rends.pop()
            self.zones[-1].rend += [RenditionSpan(r.lstrip('#'), start.start,
                                                  len(self.zones[-1].data) - start.start)
                                    for r in re.split(r'\s+', start.rendition) if r != '']
        elif tag == 'fw' and len(self.zones) > 0:
            top = self.zones.pop()
            self.seq += 1
            self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                seq=self.seq, page=self.pageID,
                                ztype=top.info.ztype, place=top.info.place,
                                text=top.data, rendition=top.rend))
        elif tag == 'gap' and len(self.zones) > 0:
            top = self.zones.pop()
            self.zones[-1].data += sub(r'^.non-Latin.*$', '@@@',
                                       sub(r'\s+$', '', sub(r'^\s+', '', top.data)))
        elif tag == 'cell' and len(self.zones) > 0:
            self.zones[-1].data += '\t'
        elif tag == 'text':
            while len(self.zones) > 0:
                self.seq += 1
                top = self.zones.pop()
                self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                    seq=self.seq, page=self.pageID,
                                    ztype=top.info.ztype, place=top.info.place,
                                    text=top.data, rendition=top.rend))
        elif tag in self.lines and len(self.zones) > 0:
            start = self.rends.pop()
            self.zones[-1].rend.append(RenditionSpan(start.rendition, start.start,
                                                     len(self.zones[-1].data) - start.start))
        elif tag in self.floats and len(self.zones) > 0:
            top = self.zones.pop()
            self.seq += 1
            self.res.append(Row(id=f'{self.pageID}z{self.seq}', book=self.book,
                                seq=self.seq, page=self.pageID,
                                ztype=top.info.ztype, place=top.info.place,
                                text=top.data, rendition=top.rend))

    def data(self, data):
        if len(self.zones) > 0:
            # remove leading whitespace only if we haven't added anything
            if len(self.zones[-1].data) == 0:
                data = re.sub(r'^\s+', '', data)
            self.zones[-1].data += re.sub(r'\n[ ]+', '\n', data)
    def comment(self, text):
        pass
    def close(self):
        res = self.res
        self.seq = -1
        self.pageID = ''
        self.zones = []
        self.rends = []
        self.res = []
        return res

def parseFile(path, content):
    book = re.sub(r'(.TEI-P5)?.xml$', '', os.path.basename(path))

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
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()

                    
