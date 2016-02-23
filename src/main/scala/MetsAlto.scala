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

import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import java.sql.Date

import collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

object MetsAlto {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MetsAlto Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.binaryFiles(args(0))
      .filter(_._1.endsWith(".zip"))
      .flatMap( x => {
        val fname = new java.io.File(new java.net.URL(x._1).toURI)
        val issue = fname.getName.replaceAll(".zip$", "")

        val zfile = new java.util.zip.ZipFile(fname)
        import scala.collection.JavaConversions._
        val (series, date, title, lang) =
          try {
            val mfile = zfile.entries.filter(_.getName.endsWith("mets.xml")).toSeq.head
            val t = scala.xml.XML.load(zfile.getInputStream(mfile))
            ((t \\ "identifier").head.text,
              (t \\ "dateIssued").head.text
                .replaceAll("""^(\d\d)\.(\d\d)\.(\d\d\d\d)$""", """$3-$2-$1"""),
              (t \\ "title").head.text,
              (t \\ "languageTerm").head.text)
          } catch {
            case e: Exception => ("", "", "", "")
          }

        zfile.entries.filter(f => f.getName.endsWith(".xml") && !f.getName.endsWith("mets.xml"))
          .map(f => {
            val t = scala.xml.XML.load(zfile.getInputStream(f))
            val res = (t \\ "TextLine").map { line =>
              (line \ "_").map({ e =>
                if ( e.label == "String" ) {
                  (e \ "@CONTENT").text
                } else if ( e.label == "SP" ) {
                  " "
                } else if ( e.label == "HYP" ) {
                  "\u00ad"
                }
              })
                .mkString("")
            }
            (f.getName.replaceAll(".xml$", ""), issue, series, date,
              title, lang, res.mkString("\n"))
          })
      }
    )
      .toDF("id", "issue", "series", "date", "title", "lang", "text")
      .write.save(args(1))
  }
}
