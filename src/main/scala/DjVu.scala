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

import com.databricks.spark.xml.XmlInputFormat

import org.apache.hadoop.io.{LongWritable,Text}
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import java.sql.Date

import collection.JavaConversions._
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer

object DjVu {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DjVu Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<OBJECT>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</OBJECT>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    val pages = sc.newAPIHadoopFile(args(0),
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])
      .map( x => {
        val res = new StringBuilder
        val t = scala.xml.XML.loadString(x._2.toString)
          (t \\ "LINE").foreach { line => {
            var first = true
              (line \ "WORD").foreach({ word => {
                if ( first ) {
                  first = false
                } else {
                  res.append(" ")
                }
                res.append(word.text.replaceAll("<", "&lt;"))
              }
              })
            res.append("\n")
          }
          }
        ((t \ "@data").text, res.toString)
      })
      .toDF("id", "text")
      .write.json(args(1))
  }
}

