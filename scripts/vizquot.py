import argparse
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Visualize quotations',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('corpusPath', metavar='<corpus path>', help='corpus path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    shards = 20

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    corpus = spark.read.load(config.corpusPath
                 ).withColumn('uid', xxhash64('id'))
    
    refs = corpus.filter(col('ref') == 1
                ).withColumn('work', regexp_replace('book', r'\.[^\.]+$', '')
                ).filter(col('work') == 'urn:cts:greekLit:tlg0003.tlg001')
    #                ).filter(col('work') == 'urn:cts:latinLit:phi0914.phi001')

    sequence = refs.groupBy('work'
                  ).agg(f.array_join(sort_array(collect_list(struct('pos', 'text')))['text'],
                                     '').alias('data')
                  ).select('work', lit('sequence.js').alias('file'),
                           concat(lit('var sequence = '),
                                  f.to_json(struct(lit('Demo').alias('name'), 'data')),
                                  lit(';\n')).alias('content'))

    offset = udf(lambda parts: get_offsets(parts), 'array<struct<pos: int, offset: int>>')

    offs = refs.groupBy('work'
              ).agg(offset(sort_array(collect_list(struct('pos', f.length('text').alias('tlen'))))).alias('off')
              ).select('work', f.explode('off').alias('off')
              ).select('work', col('off.*'))

    works = refs.select('uid', 'work', 'pos'
               ).join(offs, ['work', 'pos'])

    books = corpus.filter(col('ref') == 0
                 ).select(col('uid').alias('uid2'),
                          'book', 'title', 'author',
                          col('language_gen').alias('lang'),
                          col('date1_src').alias('date'),
                          col('pos').alias('seq'))

    track_name = udf(lambda author, title, date: f'{author}, {title} [{date}]')

    tracks = spark.read.load(config.inputPath
                 ).select('uid', 'uid2',
                          f.monotonically_increasing_id().alias('id'),
                          col('begin').alias('start'), col('end').alias('stop'),
                          regexp_replace(translate('alg.s2', '-', ''), '\n', '<br/>').alias('pre'),
                          col('alg.matches')
                 ).join(works, 'uid'
                 ).join(books, 'uid2'
                 ).groupBy('work', col('book').alias('id'), 'lang', 'date',
                           track_name('author', 'title', 'date').alias('name')
                 ).agg(collect_list(struct((col('start') + col('offset')).alias('start'),
                                           (col('stop') + col('offset')).alias('stop'),
                                           'pre', 'id',
                                           concat(f.lower('book'), lit('&seq='),
                                                  col('seq').cast('string')).alias('url'),
                                           concat(lit('p. '), col('seq').cast('string')).alias('sloc'))
                                    ).alias('notes'),
                       f.count('start').alias('notecount'))

    lang_name = udf(lambda lang: langs.get(lang, lang))

    langGroups = tracks.filter("lang = 'eng' OR lang = 'deu' OR lang = 'fra' OR lang = 'ita' OR lang = 'lat' OR lang = 'ell'"
                      ).groupBy('work', col('lang').alias('id')
                      ).agg(f.sum('notecount').alias('notecount'),
                            f.collect_set('id').alias('trackIds')
                      ).withColumn('visible', lit(True)
                      ).withColumn('name', lang_name('id'))

    groups = langGroups.groupBy('work', lit('groups.js').alias('file')
                      ).agg(concat(lit('var groups =\n'), f.to_json(sort_array(collect_list(struct('name', 'id', 'trackIds', 'notecount', 'visible')))), lit(';\n')).alias('content'))
    
    tracks.groupBy('work', f.pmod(xxhash64('id'), shards).cast('string').alias('shard')
         ).agg(collect_list(struct('id', 'name', 'lang', 'date',
                            lit('https://babel.hathitrust.org/cgi/pt?id=hvd.').alias('base'),
                            lit(False).alias('visible'),
                            'notecount', 'notes')).alias('tracks')
         ).select('work', concat(lit('tracks'), 'shard', lit('.js')).alias('file'),
                  concat(lit('var tracks'), 'shard', lit(' =\n'),
                         f.to_json('tracks'), lit(';\n')).alias('content')
         ).union(sequence
         ).union(groups
         ).write.json(config.outputPath, mode='overwrite')
    
    spark.stop()
