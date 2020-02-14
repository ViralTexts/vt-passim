# vt-passim

This repository contains helper scripts developed by the Viral Texts Project to import data and process results.

## Installation

These scripts rely on [Apache Spark](http://spark.apache.org) to manage parallel computations, either on a single machine or a cluster.  If you don't already have access to Spark, we recommend downloading a precompiled, binary distribution of Spark, unpacking it, and adding the `bin` subdirectory to your `PATH`.

To compile the code in this repository, we use [sbt](http://www.scala-sbt.org/).  Run the following command in the top level:
```
$ sbt package
```

This should produce a runnable .jar in `target/scala_*/vtpassim*.jar`. (The exact filename will depend on the version of Scala and passim that you have.)

The `bin` subdirectory of the vt-passim distribution contains executable shell scripts such as `vtrun`.  We recommend adding this subdirectory to your `PATH`.

## Format Conversions with `vtrun`

The vt-passim repository contains several Spark applications to convert data from various format into the record structure required by passim.  These conversion scripts are mostly invoked with the `vtrun` executable script.

The passim software for text reuse analysis takes as input a series of records representing individual documents.  Minimally, each record requires a unique identifier (`id`) and textual content (`text`).  In JSON format this looks like:
```
{"id": "d1", "text": "This is text."}
```
You may change the names of these fields when invoking passim.

To convert, for instance, a directory of .hocr files to passim's record format using the `HOCR` class, do:
```
$ vtrun HOCR <input> <output>
```

By default, this will convert the data into records in the binary parquet format used by Apache Spark.  To request that Spark output data in the JSON format, append the following option to the `SPARK_SUBMIT_ARGS` environment variable:
```
$ SPARK_SUBMIT_ARGS='--conf spark.sql.sources.default=json' vtrun HOCR <input> <output>
```

Some useful format conversion classes are:

Class | Description
----- | -----------
AltoPages | ALTO format output by many OCR systems
ChronAm | .tar file containing multiple .xml files in ALTO format as used by Chronicling America
DjVu | Deja Vu format as used by the Internet Archive
HOCR  | hOCR format output by many OCR systems
TCPPages | TEI XML as used by the Text Creation Partnership

Many of these formats allow multiple pages per file.  These conversion classes output one record per page.
