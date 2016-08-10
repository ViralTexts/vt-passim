package vtpassim

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import com.databricks.spark.xml.XmlInputFormat

import org.apache.hadoop.io.{LongWritable,Text}

import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

object LCMerge {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LCMerge Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val meta = sc.wholeTextFiles(args(0))
    val df = meta.map(_._2).map(x => {
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
        (t \ "Description" \ "publisher").text,
        links
          .map { z => nameId(bareName(z)) })
    }).toDF("sid", "series", "title", "lang", "placeOfPublication", "publisher", "links")

    df.write.json(args(1))
  }
}
