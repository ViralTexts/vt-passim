import argparse, os, re, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, posexplode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree
from re import sub

ns = {'': 'http://www.tei-c.org/ns/1.0'}

def IGChunks(data):
    res = []
    try:
        tree = etree.parse(BytesIO(data.encode()))
        text = tree.find(".//div[@type='edition']", namespaces=ns)
        raw = etree.tostring(text, encoding='unicode')

        id = tree.findtext(".//idno[@type='localId']", namespaces=ns).strip()

        raw = sub(r'\s*<lb[ /][^>]*>\s*', '\n',
                  sub(r'<hi rend="(smallit|sup|sup italic)">[^<]+</hi>', '\n\n', raw))
        raw = sub(r'[\-‒–]\n', '', raw)

        raw = sub(r'</?[A-Za-z][^>]*>', '', raw)

        raw = sub(r'&lt;', '<', sub(r'&gt;', '>', raw))
        raw = sub(r'\s*[–‒‒\.¯]\s*', '\n\n', raw) # gaps
        raw = sub(r'\]\[', '', raw)

        chunks = [sub(r'\[$', '', sub(r'^\]', '', sub(r'\n+', ' ', chunk))).strip()
                  for chunk in re.split('\n\n+', raw)]
        pos = 0
        for chunk in chunks:
            ## Filter out empties and non-Greek (N.B.: h is heta in epigraphy)
            if (chunk != None and chunk != ""
                and not re.search(r'[A-Za-gi-z]', chunk) and re.search(r'[Α-Ωα-ω]', chunk)):
                res.append((id, pos, chunk))
                pos += 1
    except:
        print('# Error parsing', file=sys.stderr)
        res = []
    return res

def getCite(fname):
    return sub(r'%20', ' ', os.path.basename(fname))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='IG import',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    wd = os.getcwd()
    wd = wd if wd.startswith('file:') else 'file:' + wd

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    ig_chunks = udf(lambda data: IGChunks(data), 'array<struct<cite: string, chunk: int, text: string>>')
    
    spark.read.load(config.inputPath, format='text', wholetext='true', recursiveFileLookup='true',
        ).select(f.explode(ig_chunks('value')).alias('info')
        ).select('info.*'
        ).sort('cite', 'chunk'
        ).write.json(config.outputPath, mode='overwrite')
                 
