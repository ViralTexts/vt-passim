import argparse, os, re, math
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, col, collect_list, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from PIL import Image
from kraken.lib import models
from kraken.rpred import rpred

def ocrLines(bcmodel, text, pages):
    if text == None:
        return None
    off = 0
    regions = pages[0].regions
    imfile = pages[0].id
    book = re.sub(r'_\d+$', '', imfile)
    # imfile = '/work/nulab/corpora/rowell/' + os.path.join('raw', book, book + '_jp2', imfile + '.jp2')
    base = '/work/proj_cssh/nulab/corpora/chroniclingamerica/data/batches/'
    impath = base + imfile
    i = 0
    spans = []
    boxes = []
    blines = []
    try:
        im = Image.open(impath)
        iw, ih = im.size
        S = 1 / round(pages[0].width / iw)
    except:
        im = None
        S = 1
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
        x1, y1, x2, y2 = x1*S, y1*S, x2*S, y2*S
        spans.append((line, pos1, off-pos1))
        boxes.append([x1, y1, x2, y2])
        blines.append({'baseline': [(x1, y2), (x2, y2)],
                       'tags': {'type': 'default'}, 'split': None,
                       'boundary': [(x1, y1), (x1, y2), (x2, y2), (x2, y1)]})

    # seg = {'text_direction': 'horizontal-lr', 'type': 'boxes', 'script_detection': False, 'boxes': boxes}
    baseline_seg = {'text_direction': 'horizontal-lr', 'type': 'baselines',
                    'script_detection': False,
                    'lines': blines, 'base_dir': None, 'tags': False}
    res = []
    if im != None:
        pred = rpred(bcmodel.value, im, baseline_seg)
        ocr = list(pred)
    else:
        for s in spans:
            res.append(('', s[0], s[1], s[2], None, None, None, None))
        return res

    # print('# recs: spans=', len(spans), '; boxes=', len(boxes), '; ocr=', len(ocr), '; ',
    #       len([x for x in spans if x[0] != '']))

    print("# spans =", len(spans))
    print("# boxes =", len(boxes))
    i, j = 0, 0
    off = 0
    while i < len(spans):
        span = spans[i]
        start = off
        if span[0] == '':
            res.append(('\n', span[0], start, 0, None, None, None, None))
        else:
            box = boxes[j]
            trans = ocr[j].prediction
            off += len(trans)
            # print('# pred: ', ocr[j].prediction)
            res.append((trans + '\n', span[0], start, off-start,
                        int(box[0]), int(box[1]), int(box[2]-box[0]), int(box[3]-box[1])))
            j += 1
        off += 1
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

    spark.read.load(config.inputPath
#        ).repartition(500
        ).withColumn('lines', ocr_lines('text', 'pages')
        ).withColumn('text', f.array_join('lines.text', '')
        ).withColumn('pages',
                     array(struct(col('pages')[0]['id'],
                                  col('pages')[0]['seq'],
                                  col('pages')[0]['width'],
                                  col('pages')[0]['height'],
                                  col('pages')[0]['dpi'],
                                  f.transform(f.filter('lines', lambda r: r.length > 0),
                                              lambda r: struct(r.start, r.length,
                                                               struct(r.x, r.y, r.w, r.h, r.h.alias('b')).alias('coords'))).alias('regions')))
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()


