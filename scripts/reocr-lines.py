import argparse, os, re, math
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, col, collect_list, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from PIL import Image
from kraken.lib import models
from kraken.rpred import rpred

def ocrLines(bcmodel, text, pages):
    off = 0
    regions = pages[0].regions
    imfile = pages[0].id
    book = re.sub(r'_\d+$', '', imfile)
    imfile = os.path.join('raw', book, book + '_jp2', imfile + '.jp2')
    i = 0
    spans = []
    boxes = []
    for line in text.splitlines(keepends=True):
        pos1 = off
        off += len(line)
        sline = line.rstrip()
        pos2 = pos1 + len(sline)
        if pos1 == pos2:
            spans.append(('', pos1, off-pos1))
            continue
        while i < len(regions) and (regions[i].start + regions[i].length) < pos1:
            i += 1
        x1, y1, x2, y2 = math.inf, math.inf, 0, 0
        while i < len(regions) and regions[i].start < pos2:
            coords = regions[i].coords
            x1, y1 = min(x1, coords.x), min(y1, coords.y)
            x2, y2 = max(x2, coords.x + coords.w), max(y2, coords.y + coords.h)
            i += 1
        spans.append((sline, pos1, off-pos1))
        boxes.append([x1, y1, x2, y2])

    seg = {'text_direction': 'horizontal-lr', 'script_detection': False, 'boxes': boxes}
    im = Image.open(imfile)
    pred = rpred(bcmodel.value, im, seg)
    ocr = list(pred)

    print('# recs: spans=', len(spans), '; boxes=', len(boxes), '; ocr=', len(ocr), '; ',
          len([x for x in spans if x[0] != '']))

    res = []
    i, j = 0, 0
    while i < len(spans):
        span = spans[i]
        if span[0] == '':
            res.append(('', span[0], span[1], span[2], None, None, None, None))
        else:
            box = boxes[j]
            print('# pred: ', ocr[j].prediction)
            res.append((ocr[j].prediction, span[0], span[1], span[2],
                        box[0], box[1], box[2]-box[0], box[3]-box[1]))
            j += 1
        i += 1
    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Re-OCR pre-segmented lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('modelPath', metavar='<model path>', help='model path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Re-OCR pre-segmented lines').getOrCreate()

    model = models.load_any(config.modelPath)

    bcmodel = spark.sparkContext.broadcast(model)

    ocr_lines = udf(lambda text, pages: ocrLines(bcmodel, text, pages),
                    'array<struct<text: string, orig: string, start: int, length: int, x: int, y: int, w: int, h: int>>').asNondeterministic()

    spark.read.json(config.inputPath
        ).withColumn('lines', ocr_lines('text', 'pages')
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()

