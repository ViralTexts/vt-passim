package vtpassim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy

import com.databricks.spark.xml.XmlInputFormat

import org.apache.hadoop.io.{LongWritable,Text}
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import java.sql.Date

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
      val lccn = bareName((t \ "Description" \ "describes" \ s"@{$ns}resource") text)

      val links = ArrayBuffer[String]()
      val nodes = (t \ "Description" \ "_")
      links ++= nodes.filter(_.label == "relation").map { z => ((z \ s"@{$ns}resource") text) }
      val successors = nodes.filter(_.label == "successor").map { z => ((z \ s"@{$ns}resource") text) }
      if ( successors.size == 1 ) links ++= successors
      val pred = nodes.filter(_.label == "successorOf").map { z => ((z \ s"@{$ns}resource") text) }
      if ( pred.size == 1 ) links ++= pred

      (nameId(lccn),
        lccn,
        ((t \ "Description" \ "title") text),
        ((t \ "Description" \ "language" \ s"@{$ns}resource") text)
          .replaceFirst("http://www.lingvoj.org/lang/", ""),
        ((t \ "Description" \ "placeOfPublication") text),
        ((t \ "Description" \ "publisher") text),
        links
          .map { z => nameId(bareName(z)) })
    }).toDF("sid", "id", "title", "lang", "placeOfPublication", "publisher", "links")

    df.cache()

    df.write.json(args(1))

  //   val eg = Graph.fromEdgeTuples(
  //     df.select("sid", "links").flatMap {
  //       case Row(id: Long, links: Seq[_]) => {
  //         Array((id,id)) ++ links.asInstanceOf[Seq[Long]].map { n => (id, n) }
  //       }
  //     }, 0L, Some(PartitionStrategy.RandomVertexCut))

  //   val cc = eg.stronglyConnectedComponents(100).vertices

  //   val connect = cc.toDF("sid", "group").join(df, "sid")
  //   connect.cache()

  //   val groups = connect.sort('group, 'sid).groupBy("group")
  //     .agg(first('id) as "id", first('title) as "title", first('lang) as "lang")

  //   groups.select('group, 'id).join(connect, "group").sort('group, 'sid)
  //     .coalesce(1).write.json(args(2))
  }
}
