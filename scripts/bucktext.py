from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, translate

buck = {
    '\u0621': "'",
    '\u0622': "|",
    '\u0623': ">",
    '\u0624': "&",
    '\u0625': "<",
    '\u0626': "}",
    '\u0627': "A",
    '\u0628': "b",
    '\u0629': "p",
    '\u062A': "t",
    '\u062B': "v",
    '\u062C': "j",
    '\u062D': "H",
    '\u062E': "x",
    '\u062F': "d",
    '\u0630': "*",
    '\u0631': "r",
    '\u0632': "z",
    '\u0633': "s",
    '\u0634': "$",
    '\u0635': "S",
    '\u0636': "D",
    '\u0637': "T",
    '\u0638': "Z",
    '\u0639': "E",
    '\u063A': "g",
    '\u0640': "_",
    '\u0641': "f",
    '\u0642': "q",
    '\u0643': "k",
    '\u0644': "l",
    '\u0645': "m",
    '\u0646': "n",
    '\u0647': "h",
    '\u0648': "w",
    '\u0649': "Y",
    '\u064A': "y",
    '\u064B': "F",
    '\u064C': "N",
    '\u064D': "K",
    '\u064E': "a",
    '\u064F': "u",
    '\u0650': "i",
    '\u0651': "~",
    '\u0652': "o",
    '\u0660': "0",
    '\u0661': "1",
    '\u0662': "2",
    '\u0663': "3",
    '\u0664': "4",
    '\u0665': "5",
    '\u0666': "6",
    '\u0667': "7",
    '\u0668': "8",
    '\u0669': "9",
    '\u0670': "`",
    '\u0671': "{",
    '\u067E': "P",
    '\u0686': "J",
    '\u06A4': "V",
    '\u06AF': "G",
    '\u06CC': "y",
    '\u06F0': "0",
    '\u06F1': "1",
    '\u06F2': "2",
    '\u06F3': "3",
    '\u06F4': "4",
    '\u06F5': "5",
    '\u06F6': "6",
    '\u06F7': "7",
    '\u06F8': "8",
    '\u06F9': "9"
}

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: bucktext.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Transliterate Text Field').getOrCreate()
    spark.read.load(sys.argv[1])\
              .withColumn("text",
                          translate('text',
                                    ''.join(buck.keys()),
                                    ''.join(buck.values()))) \
              .write.save(sys.argv[2])
    spark.stop()

                              # translate('text',
                              #       '\u0621\u0627\u0628\u0629\u062a\u062b\u062c\u062d\u062e\u062f\u0630\u0631\u0632\u0633\u0634\u0635\u0636\u0637\u0638\u0639\u063a\u0641\u0642\u0643\u0644\u0645\u0646\u0647\u0648\u0649\u064a\u064b\u064c\u064d\u064e\u064f\u0650\u0651\u0652\u0653\u0654\u0655\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669',
                              #       "'AbptvjHxd*rzs$SDTZEgfqklmnhwYyFNKaui~o=^_0123456789"))\

