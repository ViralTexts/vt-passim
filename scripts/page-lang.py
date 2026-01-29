import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from pyfranc import franc
from langchain_text_splitters import RecursiveCharacterTextSplitter

# cand = ['eng', 'lat', 'deu', 'fra', 'ita', 'ell', 'spa', 'rus', 'nld', 'por', 'heb',
#         'swe', 'dan', 'nor', 'hun', 'cym', 'fin', 'oci', 'ron', 'bos', 'fry',
#         'isl', 'hrv', 'slv', 'srp', 'cmn', 'jpn', 'kor',
#         'arb', 'pes', 'san', 'hye', 'mya',
#         'cat', 'slk', 'gla', 'roh',
#         'bre', 'nno', 'bul', 'mlt', 'lit', 'ydd',
#         'pol', 'ces', 'gle']

# 47 top languages where IDI and metadata languages agree over 20 times
cand = ['ben', 'bre', 'bul', 'cat', 'ces', 'cym', 'dan', 'deu', 'ell', 'eng', 'epo',
        'fin', 'fra', 'fry', 'gla', 'gle', 'haw', 'heb', 'hin', 'hrv', 'hun', 'hye', 'isl', 'ita',
        'jpn', 'kat', 'lat', 'lit', 'mar', 'mya', 'nld', 'oci', 'pol', 'por', 'roh', 'ron', 'rus',
        'san', 'slk', 'slv', 'spa', 'srp', 'swe', 'tam', 'tha', 'ukr', 'urd',
        'arb', 'cmn', 'pes', 'ydd'] # other top languages

def pageLang(s, bsplit):
    res = []
    start = 0
    splitter = bsplit.value
    for chunk in splitter.split_text(s):
        lang = franc.lang_detect(chunk, whitelist=cand)[0][0]
        res.append((start, len(chunk), len(chunk.split()), lang))
        start += len(chunk)
    return res

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Page languages',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName(parser.description).getOrCreate()

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=768,         # IDI size
        chunk_overlap=0,
        length_function=len,
        is_separator_regex=False,
    )

    bsplit = spark.sparkContext.broadcast(text_splitter)

    page_lang = udf(lambda text: pageLang(text, bsplit),
                    'array<struct<start: int, length: int, wst: int, lang: string>>')

    pages = spark.read.load(config.inputPath
        ).select('book', 'pos', explode(page_lang('text').alias('lang')).alias('chunks')
        ).select('book', 'pos', 'chunks.*'
        ).write.save(config.outputPath, mode='overwrite')

    spark.stop()
