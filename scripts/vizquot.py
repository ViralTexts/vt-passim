import argparse, json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (btrim, col, lit, regexp_replace, sort_array, split, size, rlike,
                                   collect_list, struct, xxhash64, concat, translate, udf)
import pyspark.sql.functions as f

langs = {'eng': 'English',
         'deu': 'German',
         'fra': 'French',
         'ita': 'Italian',
         'lat': 'Latin',
         'ell': 'Greek',
         'grc': 'Ancient Greek'}

def get_offsets(parts):
    off = 0
    res = []
    for part in parts:
        res.append((part.pos, off))
        off += part.tlen
    return res

## Return a string of the JSON data so we don't need to do type inference
def locStruct(locs):
    hier = {}
    res = []
    prev = []
    stack = []
    for loc in locs:
        # if loc.start >= 2565:
        #     break
        parts = (loc.loc.split(':')[4]).split('.')
        for i in range(len(parts)):
            if i >= len(prev):
                stack.append([])
                prev.append(parts[i])
            elif i < (len(parts) - 1) and prev[i] != parts[i]:
                while len(stack) > (i+1):
                    desc = stack.pop()
                    prev.pop()
                    ## Rely on locs being sorted by start
                    stack[-1].append({'id': '.'.join(prev),
                                      'name': prev[-1],
                                      'start': desc[0]['start'],
                                      'stop': desc[-1]['stop'],
                                      'children': desc})
        # prev[-1] = parts[-1]
        prev = parts
        stack[-1].append({'id': '.'.join(prev),
                          'name': prev[-1],
                          'start': loc.start,
                          'stop': loc.start + loc.length - 1})
    while len(stack) > 1:
        desc = stack.pop()
        prev.pop()
        stack[-1].append({'id': '.'.join(prev),
                          'name': prev[-1],
                          'start': desc[0]['start'],
                          'stop': desc[-1]['stop'],
                          'children': desc})

    return ('var structure = [\n' +
            json.dumps({'id': 'book', 'name': 'book', 'style': 'ac', 'notes': stack[0]}) +
            '];')

def makeIndex(name, tfiles):
    tinclude = '\n'.join(f'<script src="{file}" language="javascript"></script>' for file in tfiles)
    tconcat = ','.join(f'...{file.replace(".js", "")}' for file in tfiles)

    return f"""<html>
<head>
<title>{name}</title>
<meta charset="utf-8" />
<!-- 3rd party libraries -->
<script src="../../../highbrow/lib/js/jquery.min.js"></script>
<script src="../../../highbrow/lib/js/jquery.dataTables.min.js" language="javascript" type="text/javascript"></script> 
<script src="../../../highbrow/lib/js/jquery-ui.min.js" type="text/javascript"></script> 
<link href="../../../highbrow/lib/css/jquery-ui.css" type="text/css" rel="stylesheet" /> 
<script src="../../../highbrow/lib/js/processing.min.js" language="javascript"></script>
<script src="../../../highbrow/lib/js/jquery.ba-bbq.min.js" language="javascript"></script>

<!-- highbrow code -->
<script src="../../../highbrow/js/highbrow.main.js"></script>
<script src="../../../highbrow/js/highbrow.settings.js"></script>
<script src="../../../highbrow/js/highbrow.search.js"></script>
<script src="../../../highbrow/js/highbrow.spanel.js"></script>
<script src="../../../highbrow/js/highbrow.npanel.js"></script>
<script src="../../../highbrow/js/highbrow.linker.js"></script>
<script src="../../../highbrow/js/highbrow.map.js"></script>

<link rel="stylesheet" href="../../../highbrow/highbrow.css" type="text/css" />

<!-- highbrow data and configuration -->
<script src="sequence.js" language="javascript"></script>
<script src="structure.js" language="javascript"></script>
{tinclude}
<script src="groups.js" language="javascript"></script>

</head>

<body>

<div id="HB_container"></div>
<script language="javascript">
var hbconf= {{}};
hbconf.sequence = sequence;
hbconf.tracks = [{tconcat}];
hbconf.groups = groups;
hbconf.structure = structure;
hbconf.container = "HB_container"
var hb = new Highbrow(hbconf);
</script>

</body>

</html>
    """

def trackName(author, title, date, truncate=40):
    res = ''
    if author != None and author != '':
        res += author + ', '
    res += title[0:truncate]
    if len(title) > truncate:
        res += '...'
    if date != None and date != '':
        res += ' [' + date + ']'
    return res
                
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Visualize quotations',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('psgPath', metavar='<psg path>', help='psg path')
    parser.add_argument('corpusPath', metavar='<corpus path>', help='corpus path')
    parser.add_argument('metaPath', metavar='<meta path>', help='meta path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    shards = 20

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    corpus = spark.read.load(config.corpusPath, mergeSchema=True
                 ).withColumn('uid', xxhash64('id'))

    meta = spark.read.csv(config.metaPath, header=True, escape='"'
               ).select(col('urn').alias('work'),
                        concat(col('author'), lit('. '), col('title')).alias('name'))

    refs = corpus.filter(col('ref') == 1
                ).withColumn('work', regexp_replace('book', r'\.[^\.]+$', '')
                ) #.filter(col('work') == 'urn:cts:greekLit:tlg0003.tlg001')
    #                ).filter(col('work') == 'urn:cts:latinLit:phi0914.phi001')

    sequence = refs.groupBy('work'
                  ).agg(f.array_join(sort_array(collect_list(struct('pos', 'text')))['text'],
                                     '').alias('data')
                  ).join(meta, 'work'
                  ).select('work', lit('sequence.js').alias('file'),
                           concat(lit('var sequence = '),
                                  f.to_json(struct('name', 'data')),
                                  lit(';')).alias('content'))

    offset = udf(lambda parts: get_offsets(parts), 'array<struct<pos: int, offset: int>>')

    offs = refs.groupBy('work'
              ).agg(offset(sort_array(collect_list(struct('pos', f.length('text').alias('tlen'))))).alias('off')
              ).select('work', f.explode('off').alias('off')
              ).select('work', col('off.*'))

    loc_struct = udf(lambda locs: locStruct(locs))

    structure = refs.join(offs, ['work', 'pos']
                   ).select('work', 'pos',
                            f.transform('locs',
                                        lambda r: r.withField('start', r.start + col('offset'))).alias('locs')
                   ).groupBy('work', lit('structure.js').alias('file')
                   ).agg(loc_struct(f.flatten(sort_array(collect_list(struct('pos', 'locs')))['locs'])).alias('content'))

    works = refs.select('uid', 'work', 'pos'
               ).join(offs, ['work', 'pos'])

    books = corpus.filter(col('ref') == 0
                 ).select(col('uid').alias('uid2'),
                          'book', 'title', 'author',
                          col('language_gen').alias('lang'),
                          col('date1_src').alias('date'),
                          col('pos').alias('seq'))

    track_name = udf(lambda author, title, date: trackName(author, title, date))

    tracks = spark.read.load(config.psgPath
                 ).select('uid', 'uid2',
                          f.monotonically_increasing_id().alias('id'),
                          col('begin').alias('start'), col('end').alias('stop'),
                          regexp_replace(translate('alg.s2', '-', ''), '\n', '<br/>').alias('pre'),
                          col('alg.matches')
                 ).join(works, 'uid'
                 ).join(books, 'uid2'
                 # ).filter((col('start') + col('offset')) < 2565
                 ).groupBy('work', col('book').alias('id'), 'lang', 'date',
                           track_name('author', 'title', 'date').alias('name')
                 ).agg(sort_array(
                     collect_list(struct((col('start') + col('offset')).alias('start'),
                                         (col('stop') + col('offset')).alias('stop'),
                                         # 'pre',
                                         lit('').alias('pre'),
                                         'id',
                                         concat(f.lower('book'), lit('&seq='),
                                                col('seq').cast('string'),
                                                # lit('&q1='),
                                                # translate(regexp_replace('pre', '<br/>', ' '), ' &', '+')
                                                ).alias('url'),
                                         concat(lit('p. '), col('seq').cast('string')).alias('sloc'))
                                  )).alias('notes'),
                       f.count('start').alias('notecount'))

    lang_name = udf(lambda lang: langs.get(lang, lang))

    langGroups = tracks.filter("lang = 'eng' OR lang = 'deu' OR lang = 'fra' OR lang = 'ita' OR lang = 'lat' OR lang = 'ell'"
                      ).groupBy('work', col('lang').alias('id')
                      ).agg(f.sum('notecount').alias('notecount'),
                            sort_array(f.collect_set('id')).alias('trackIds')
                      ).withColumn('visible', lit(True)
                      ).withColumn('name', lang_name('id'))

    groups = langGroups.groupBy('work', lit('groups.js').alias('file')
                      ).agg(concat(lit('var groups =\n'), f.to_json(sort_array(collect_list(struct('name', 'id', 'trackIds', 'notecount', 'visible')))), lit(';\n')).alias('content'))

    make_index = udf(lambda name, tfiles: makeIndex(name, tfiles))

    trackShards = tracks.groupBy('work',
                                 f.pmod(xxhash64('id'), shards).cast('string').alias('shard')
                       ).agg(collect_list(
                           struct('id', 'name', 'lang', 'date',
                                  lit('https://babel.hathitrust.org/cgi/pt?id=hvd.').alias('base'),
                                  lit(False).alias('visible'),
                                  'notecount', 'notes')).alias('tracks')
                       ).select('work', concat(lit('tracks'), 'shard', lit('.js')).alias('file'),
                                concat(lit('var tracks'), 'shard', lit(' =\n'),
                                       f.to_json(sort_array('tracks')), lit(';')).alias('content'))

    index = trackShards.groupBy('work'
                      ).agg(sort_array(f.collect_set('file')).alias('files')
                      ).join(meta, 'work'
                      ).select('work', lit('index.html').alias('file'), make_index('name', 'files'))
    
    trackShards.union(sequence
              ).union(structure
              ).union(groups
              ).union(index
              ).withColumn('dir', translate(regexp_replace('work', r'^urn:cts:', ''), '.:', '//')
              ).write.json(config.outputPath, mode='overwrite')
    
    spark.stop()
