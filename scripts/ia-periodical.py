import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (col, collect_list, lit, sort_array, struct, udf)
import pyspark.sql.functions as f

def catPages(pagea):
    text = ''
    pages = []
    for p in pagea:
        cur = p.page.asDict(True)
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
    parser = argparse.ArgumentParser(description='IA Periodical',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--max-chars', type=int, default=2000000,
                        help='Maximum characters')
    parser.add_argument('-p', '--max-pages', type=int, default=1000,
                        help='Maximum pages')

    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('IA Periodical').getOrCreate()

    corpus = spark.read.load(config.inputPath).withColumnRenamed('book', 'issue')

    goods = corpus.groupBy('issue'
                ).agg(f.count('id').alias('pp'), f.sum(f.length('text')).alias('tlen')
                ).filter( (col('pp') <= config.max_pages)
                          & (col('tlen') <= config.max_chars)
                          & ~(col('issue').rlike('_(index|appendix|contents)'))
                          & ~(col('issue').rlike('\d{4}-\d{4}'))
                ).select('issue')

    cat_pages = udf(lambda pagea: catPages(pagea),
                    corpus.select('text', 'pages').schema.simpleString())

    spark.conf.set('spark.sql.shuffle.partitions', 5000)

    corpus.join(goods, 'issue', 'left_semi'
        ).groupBy('issue'
        ).agg(
            cat_pages(sort_array(collect_list(struct('seq', 'text',
                                                     col('pages')[0].alias('page'))))).alias('p')
        ).select(col('issue').alias('id'), 'issue',
                 f.concat(lit('pub_'), f.split('issue', '_')[1]).alias('series'),
                 f.regexp_replace(f.split('issue', '_')[2], r'-\d{4}.*$', ''
                                  ).cast('date').cast('string').alias('date'),
                 col('p.text'), col('p.pages')
        ).filter(col('date').isNotNull()
        ).write.save(config.outputPath)

    spark.stop()
