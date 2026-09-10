import argparse, glob, os, re, sys
from re import sub
import re
from unicodedata import normalize, combining
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (array, col, collect_list, explode, regexp_replace,
                                   sort_array, split, struct, udf)
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree

ns = {'': 'http://www.tei-c.org/ns/1.0', 'xml': 'http://www.w3.org/XML/1998/namespace'}

class DocStream(object):
    def __init__(self, fname):
        self.fname = fname
        self.buf = ''
        self.rend = [False]
    def start(self, elem, attrib):
        tag = etree.QName(elem).localname
        if tag == 'ab':
            self.rend.append(True)
        elif tag == 'ex':
            if self.rend[-1]:
                self.buf += '('
            self.rend.append(False)
        elif tag == 'supplied' or tag == 'unclear':
            if self.rend[-1]:
                self.buf += '['
            self.rend.append(False)
        elif tag == 'gap' and self.rend[-1]:
            self.buf += '[]'
        elif tag == 'space' and self.rend[-1]:
            self.buf += ' '
        elif tag == 'lb' and self.rend[-1]:
            self.buf += '\n'
        elif tag == 'desc' or tag == 'figDesc' or tag == 'note' or tag == 'rdg' or tag == 'reg':
            self.rend.append(False)
    def end(self, elem):
        tag = etree.QName(elem).localname
        if tag == 'ab':
            self.rend.pop()
        elif tag == 'ex':
            self.rend.pop()
            if self.rend[-1]:
                self.buf += ')'
        elif tag == 'supplied' or tag == 'unclear':
            self.rend.pop()
            if self.rend[-1]:
                self.buf += ']'
        elif tag == 'desc' or tag == 'figDesc' or tag == 'note' or tag == 'rdg' or tag == 'reg':
            self.rend.pop()
    def data(self, data):
        if self.rend[-1]:
            self.buf += sub(r'\n+', '', data)
    def comment(self, text):
        pass
    def close(self):
        return self.buf

def IDPParse(fname, data):
    res = []
    try:
        data = sub(r'xml:id="_[^"]*"', ' ', data)
        
        tree = etree.parse(BytesIO(data.encode()))
        id = tree.findtext("./teiHeader/fileDesc/publicationStmt/idno[@type='filename']",
                           namespaces=ns).strip()

        ed = tree.find("./text//div[@type='edition']", namespaces=ns)

        lang = ed.attrib.get('{' + ns['xml'] + '}lang', None)

        parts = ed.findall(".//div[@type='textpart']", namespaces=ns)
        nparts = len(parts) if parts != None else 1


        # # ## Do something with <hi rend="diaeresis"> ?

        parser = etree.XMLParser(target = DocStream(fname))
        raw = etree.parse(BytesIO(data.encode()), parser)

        raw = normalize('NFD', raw)
        raw = sub(r'\[\](\[\])+', '[]', raw)
        raw = sub(r'[\.,\?!;:]', '', raw)
        raw = ''.join(c for c in raw if combining(c) == 0)
        raw = raw.upper()

        raw = sub(r' [ ]+', ' ', sub(r'^\s+', '', sub(r'\s+$', '', raw)))

        res.append((id, fname, lang, nparts, raw))
    except:
        print(f'# Error parsing {fname}', file=sys.stderr)
        res = []
    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='IDP Papyri import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    idp_parse = udf(lambda fname, data: IDPParse(fname, data),
                    'array<struct<id: string, fname: string, lang: string, parts: int, text:string>>')
    
    spark.read.load(config.inputPath, format='text', wholetext='true', recursiveFileLookup='true',
        ).select(explode(idp_parse(f.input_file_name(), 'value')).alias('info')
        ).select('info.*'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
