package vtpassim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.StringBuilder
import scala.xml.pull._

object TCPPages {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TCPPages Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.binaryFiles(args(0))
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        var id = ""
        var title = ""
        var creator = ""
        var date = ""
        var img = ""
        var inPage = false
        var inHeader = false
        var seq = -1
        val buf = new StringBuilder
        val xr = new XMLEventReader(scala.io.Source.fromURL(in._1))
        xr.flatMap { event =>
          event match {
            case EvElemStart(_, "pb", attr, _) => {
              val rec = if ( inPage ) {
                Some((id + "_" + seq, id, seq, title, creator, date, img, buf.toString.trim))
              } else {
                None
              }
              inPage = true
              seq += 1
              buf.clear
              img = attr.get("facs").map(_.text).headOption.getOrElse("")
              rec
            }
            case EvElemEnd(_, "text") => {
              if ( inPage ) {
                seq += 1
                val text = buf.toString.trim
                buf.clear
                Some((id + "_" + seq, id, seq, title, creator, date, img, text))
              } else {
                None
              }
            }
            case EvElemStart(_, "teiHeader", attr, _) => {
              inHeader = true
              buf ++= "<teiHeader" + attr + ">"
              None
            }
            case EvElemEnd(_, "teiHeader") => {
              inHeader = false
              buf ++= "</teiHeader>"
              val t = scala.xml.XML.loadString(buf.toString)
              id = (t \\ "idno").filter(x => (x \ "@type").text == "DLPS")
                .map(_.text).mkString("; ")
              if ( id == "" ) id = in._1
              title = (t \\ "fileDesc" \ "titleStmt" \ "title").map(_.text).mkString("; ")
              creator = (t \\ "fileDesc" \ "titleStmt" \ "author").map(_.text).mkString("; ")
              date = (t \\ "editionStmt" \ "edition" \ "date").map(_.text).mkString("; ")
              buf.clear
              None
            }
            case EvElemStart(_, tag, attr, _) => {
              if ( inHeader ) buf ++= "<" + tag + attr + ">"
              None
            }
            case EvElemEnd(_, tag) => {
              if ( inHeader ) buf ++= "</" + tag + ">"
              None
            }
            case EvText(t) => {
              if ( inPage || inHeader ) buf ++= t
              None
            }
            case EvEntityRef(n) => {
              if ( inPage || inHeader ) buf ++= "&" + n + ";"
              None
            }
            case _ => None
          }
        }
      })
      .toDF("id", "series", "seq", "title", "creator", "date", "imgid", "text")
      .write.save(args(1))
  }
}
