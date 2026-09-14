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

def drill(q, unit, val, chunks):
    for e, cit in chunks:
        for elem in e.findall(q, namespaces=ns):
            yield (elem, cit + ':' + unit + '=' + elem.attrib.get(val, ''))

def textLocs(pref, levels, chunks):
    if len(levels) > 0:
        for lev in levels:
            q = sub(r'^/TEI/', './', lev.attrib.get('match', ''))
            unit = lev.attrib.get('unit', '')
            val = sub(r'^@', '', lev.attrib.get('use', ''))
            if q == '':
                break
            chunks = drill(q, unit, val, chunks)

    raw = ''
    locs = []

    for e, cit in chunks:
        c = sub(r'\s+$', '', sub(r'^\s+', '', ''.join(e.itertext())))
        locs.append((pref + cit, len(raw), len(c)))
        raw += c
        raw += '\n'
        # if e.tag == '{' + ns[''] + '}l':
        #     raw += '\n'
    return (raw, locs)

def WorkParse(fname, data):
    res = []
    if fname.endswith('metadata.xml'):
        return res
    try:
        tree = etree.parse(BytesIO(data.encode()))

        id = sub(r'\.xml$', '', os.path.basename(fname))
        creator = tree.findtext('./teiHeader/fileDesc/titleStmt/author', namespaces=ns).strip()
        title = tree.findtext('./teiHeader/fileDesc/titleStmt/title', namespaces=ns).strip()

        levels = tree.findall('./teiHeader/encodingDesc/refsDecl//citeStructure', namespaces=ns)

    except:
        print(f'# Error parsing {fname}', file=sys.stderr)
        return []

    work = sub(r'\.[^\.]*$', '', id)
    if levels == None or len(levels) == 0:
        raw = ''.join(tree.find('./text/body', namespaces=ns).itertext())
        return [(id, work, creator, title, raw, [(work, 0, len(raw))])]

    lev = levels[0]
    q = sub(r'^/TEI/', './', lev.attrib.get('match', ''))
    unit = lev.attrib.get('unit', '')
    val = sub(r'^@', '', lev.attrib.get('use', ''))
    tops = [(e, unit + '=' + e.attrib.get(val, '')) for e in tree.findall(q, namespaces=ns)]

    if unit == 'book':
        for book in tops:
            (raw, locs) = textLocs(work + ':', levels[1:], [book])
            res.append((id + ':' + book[1], work, creator, title, raw, locs))
    else:
        (raw, locs) = textLocs(work + ':', levels[1:], tops)
        res.append((id, work, creator, title, raw, locs))
    
    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Freed corpus import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    work_parse = udf(lambda fname, data: WorkParse(fname, data),
                    'array<struct<id: string, work: string, creator: string, title: string, text: string, locs: array<struct<loc: string, start: int, length: int>>>>')
    
    spark.read.load(config.inputPath, format='text', wholetext='true', recursiveFileLookup='true',
        ).select(explode(work_parse(f.input_file_name(), 'value')).alias('info')
        ).select('info.*'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()

