package vtpassim

import org.apache.spark.sql.SparkSession

import com.databricks.spark.xml.XmlInputFormat

import org.apache.hadoop.io.{LongWritable,Text}

import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.xml.XML

object LCMerge {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Merge LC metadata").getOrCreate()
    import spark.implicits._

    spark.sparkContext
      .hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val meta = spark.sparkContext.wholeTextFiles(args(0))
    val dpiInfo = spark.read.json(args(1))
    meta.map(_._2).map(x => {
      def bareName(s: String): String = { s.replaceFirst("#title$", "") }
      def nameId(s: String): Long = { s.replaceFirst("^[a-z/]+", "").toLong }
      val t = XML.loadString(x)
      val ns = t.getNamespace("rdf")
      val lccn = bareName((t \ "Description" \ "describes" \ s"@{$ns}resource").text)

      val links = ArrayBuffer[String]()
      val nodes = (t \ "Description" \ "_")
      links ++= nodes.filter(_.label == "relation").map { z => ((z \ s"@{$ns}resource").text) }
      val successors = nodes.filter(_.label == "successor").map { z => ((z \ s"@{$ns}resource").text) }
      if ( successors.size == 1 ) links ++= successors
      val pred = nodes.filter(_.label == "successorOf").map { z => ((z \ s"@{$ns}resource").text) }
      if ( pred.size == 1 ) links ++= pred

      (nameId(lccn),
        lccn,
        (t \ "Description" \ "title").text,
        (t \ "Description" \ "language")
          .map { x => (x \ s"@{$ns}resource").text.replaceFirst("http://www.lingvoj.org/lang/", "") },
        (t \ "Description" \ "placeOfPublication").text,
        Try((t \ "Description" \ "coverage")
          .map { n => (n \ s"@{$ns}resource").text }
          .filter { _.startsWith("http://dbpedia.org") }.head).getOrElse(null),
        (t \ "Description" \ "publisher").text,
        links
          .map { z => nameId(bareName(z)) })
    }).toDF("sid", "series", "title", "lang", "placeOfPublication", "coverage", "publisher", "links")
      .drop("links")
      .join(dpiInfo, Seq("series"), "left_outer")
      .write.json(args(2))
    spark.stop()
  }
}
