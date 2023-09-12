import argparse, os, re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, col, collect_list, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from xml.sax.saxutils import escape

def makeAlto(outpath, id, img, width, height, lines):
    book = re.sub(r'_\d+$', '', id)
    path = os.path.abspath(os.path.join('raw', book, book + '_jp2', img + '.jp2'))
    res = f"""<?xml version="1.0" encoding="UTF-8"?>
<alto xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://www.loc.gov/standards/alto/ns-v4#"
      xsi:schemaLocation="http://www.loc.gov/standards/alto/ns-v4# http://www.loc.gov/standards/alto/v4/alto-4-3.xsd">
    <Description>
        <MeasurementUnit>pixel</MeasurementUnit>
        <sourceImageInformation>
            <fileName>{path}</fileName>
        </sourceImageInformation>
    </Description>
    <Layout>
        <Page WIDTH="{width}" HEIGHT="{height}" PHYSICAL_IMG_NR="0" ID="page_0">
            <PrintSpace HPOS="0" VPOS="0" WIDTH="{width}" HEIGHT="{height}">
                <TextBlock ID="textblock_1">
"""

    seq = 0
    for line in lines:
        x1, y1, x2, y2 = line.x, line.y, line.x+line.w, line.y+line.h
        content = escape(line.text).replace('"', '&quot;')
        res += f"""
                    <TextLine ID="line_{seq}" BASELINE="{x1} {y2} {x2} {y2}" HPOS="{x1}" VPOS="{y1}" WIDTH="{line.w}" HEIGHT="{line.h}"  >
                        <Shape>
                            <Polygon POINTS="{x1} {y1} {x1} {y2} {x2} {y2} {x2} {y1}"/>
                        </Shape>
                        <String CONTENT="{content}" HPOS="{x1}" VPOS="{y1}" WIDTH="{line.w}" HEIGHT="{line.h}"/>
                    </TextLine>
    """
        seq += 1

    res += """
                </TextBlock>
            </PrintSpace>
        </Page>
    </Layout>
</alto> 
"""

    outfile = os.path.join(outpath, book, img + '.xml')
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    with open(outfile, 'w') as f:
        f.write(res)

    return len(lines)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Patch Alto',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', '--filter', type=str,
                        default='matchRate > 0.5 AND leadGap = 0 AND tailGap = 0 AND maxGap < 4',
                        help='Filter training lines')
    parser.add_argument('-m', '--min-lines', type=int, default=1,
                        help='Minimum number of lines per page')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Patch Alto').getOrCreate()

    make_alto = udf(lambda id, img, width, height, lines:
                    makeAlto(config.outputPath, id, img, width, height, lines),
                    'int').asNondeterministic()

    raw = spark.read.json(config.inputPath).filter(config.filter)

    lines = raw.groupBy('id', 'img', 'width', 'height',
              ).agg(sort_array(collect_list(struct('begin', 'x', 'y', 'w', 'h',
                                                f.trim('srcText').alias('text')))).alias('lines')
              ).filter(f.size('lines') >= config.min_lines
              ).withColumn('alines', make_alto('id', 'img', 'width', 'height', 'lines')
              ).select(f.sum('alines').alias('lines')).collect()

    print('# lines: ', lines)

    spark.stop()
