import argparse, os, re, math
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, col, collect_list, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from PIL import Image
from kraken.lib import models
from kraken.rpred import rpred

def ocrLines(bcmodel, text, page, base):
    if text == None:
        return None
    regions = page.regions
    # book = re.sub(r'_\d+$', '', imfile)
    # imfile = '/work/nulab/corpora/rowell/' + os.path.join('raw', book, book + '_jp2', imfile + '.jp2')
    impath = os.path.join(base, page.id)
    try:
        im = Image.open(impath)
        iw, ih = im.size
        S = 1 / round(page.width / iw)
    except:
        return None

    i = 0
    spans = []
    boxes = []
    blines = []
    buf = ''
    x1, y1, x2, y2 = math.inf, math.inf, 0, 0
    while i < len(regions):
        r = regions[i]
        buf += text[r.start:(r.start+r.length)]
        x1, y1 = min(x1, r.coords.x), min(y1, r.coords.y)
        x2, y2 = max(x2, r.coords.x + r.coords.w), max(y2, r.coords.y + r.coords.h)

        if i < (len(regions) - 1):
            after = regions[i+1]
            suff = text[(r.start+r.length):after.start]
        else:
            suff = '\n'

        if suff.find('\n') > -1:
            spans.append((buf, suff))
            boxes.append([x1, y1, x2, y2])
            x1, y1, x2, y2 = x1*S, y1*S, x2*S, y2*S
            blines.append({'baseline': [(x1, y2), (x2, y2)],
                           'tags': {'type': 'default'}, 'split': None,
                           'boundary': [(x1, y1), (x1, y2), (x2, y2), (x2, y1)]})
            buf = ''
            x1, y1, x2, y2 = math.inf, math.inf, 0, 0
        else:
            buf += suff

        i += 1

    # seg = {'text_direction': 'horizontal-lr', 'type': 'boxes', 'script_detection': False, 'boxes': boxes}
    baseline_seg = {'text_direction': 'horizontal-lr', 'type': 'baselines',
                    'script_detection': False,
                    'lines': blines, 'base_dir': None, 'tags': False}
    res = []
    pred = rpred(bcmodel.value, im, baseline_seg)
    ocr = list(pred)

    # print('# recs: spans=', len(spans), '; boxes=', len(boxes), '; ocr=', len(ocr), '; ',
    #       len([x for x in spans if x[0] != '']))

    i = 0
    off = 0
    while i < len(spans):
        span = spans[i]
        start = off
        box = boxes[i]
        trans = ocr[i].prediction
        off += len(trans) + len(span[1])
        # print('# pred: ', ocr[j].prediction)
        res.append((trans + span[1], span[0] + span[1], start, len(trans),
                    int(box[0]), int(box[1]), int(box[2]-box[0]), int(box[3]-box[1])))
        i += 1
    return res

def catPages(pagea):
    text = ''
    pages = []
    for p in pagea:
        cur = p.pages.asDict(True)
        off = len(text)
        if 'regions' not in cur:
            cur['regions'] = []
        i = 0
        while i < len(cur['regions']):
            cur['regions'][i]['start'] += off
            i += 1
        pages.append(cur)
        text += p.text
    return {'text': text, 'pages': pages}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Re-OCR pre-segmented lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--base', type=str, default='', help='Base path for images')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('modelPath', metavar='<model path>', help='model path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Re-OCR pre-segmented lines').getOrCreate()

    model = models.load_any(config.modelPath)

    bcmodel = spark.sparkContext.broadcast(model)

    ocr_lines = udf(lambda text, page: ocrLines(bcmodel, text, page, config.base),
                    'array<struct<text: string, orig: string, start: int, length: int, x: int, y: int, w: int, h: int>>').asNondeterministic()

    raw = spark.read.load(config.inputPath)
    
    cat_pages = udf(lambda pagea: catPages(pagea),
                    raw.select('text', 'pages').schema.simpleString())

    fields = [f for f in raw.columns if (f != 'text' and f != 'pages')]

    raw.withColumn('pages', f.explode('pages')
        ).repartition(500
        ).withColumn('lines', ocr_lines('text', 'pages')
        ).withColumn('text', f.array_join('lines.text', '')
        ).withColumn('pages',
                     struct(col('pages.id'),
                            col('pages.seq'),
                            col('pages.width'),
                            col('pages.height'),
                            col('pages.dpi'),
                            f.transform(f.filter('lines', lambda r: r.length > 0),
                                        lambda r: struct(r.start, r.length,
                                                         struct(r.x, r.y, r.w, r.h,
                                                                r.h.alias('b')
                                                            ).alias('coords'))).alias('regions'))
        ).groupBy(*fields,
        ).agg(cat_pages(sort_array(collect_list(struct('pages.seq','text','pages')))).alias('p')
        ).withColumn('text', col('p.text')
        ).withColumn('pages', col('p.pages')
        ).drop('p'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
