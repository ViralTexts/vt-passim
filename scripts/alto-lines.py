import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree

ns = {'alto': 'http://www.loc.gov/standards/alto/ns-v4#'}

def textLines(s):
    text = ''
    lines = []
    if s != None and s != '':
        tree = etree.parse(BytesIO(s.encode()))
        for line in tree.findall('//alto:TextLine', namespaces=ns):
            ## This should be generalized to support lines w/ and w/o CONTENT attribute.
            start = len(text)
            for w in line.findall('.//alto:String', namespaces=ns):
                if text != '' and not text[len(text)-1].isspace():
                    text += ' '
                text += w.get('CONTENT')
            text += '\n'
            lines.append((start, len(text), line.get('ID')))
        
    return (text, lines)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Alto lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Alto lines').getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration(
        ).set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')

    text_lines = udf(lambda s: textLines(s),
                     'struct<text: string, lineIDs: array<struct<start: int, length: int, id: string>>>').asNondeterministic()

    # spark.read.text(config.inputPath, wholetext='true', recursiveFileLookup='true'
    spark.sparkContext.wholeTextFiles(config.inputPath
        ).map(lambda f: Row(id=f[0], value=f[1])
        ).toDF(
        ).withColumn('lines', text_lines('value')
        # ).select(f.input_file_name().alias('id'), text_lines('value').alias('lines')
        ).select('id', 'lines.*'
        ).write.json(config.outputPath)

    spark.stop()
