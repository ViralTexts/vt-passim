package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._
import vtpassim.pageinfo._

case class VACRecord(id: String, series: String,
  creator: String, title: String, publisher: String, placeOfPublication: String, date: String,
  text: String, locs: Array[Locus])

object VACBooks {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DTAPages Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val t = scala.xml.XML.load(in._1)
        val header = (t \\ "teiHeader")
        val id = (header \\ "idno").filter { n => (n \ "@type").text == "wright" }.text

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        val buf = new StringBuilder
        val locs = new ListBuffer[Locus]()
        var buffering = false
        var curchap = 0
        var curstart = 0
        pass.foreach { event =>
          event match {
            case EvElemStart(_, "text", attr, _) => {
              buffering = true
              None
            }
            case EvElemEnd(_, "text") => {
              if ( curchap > 0 ) {
                locs += Locus(curstart, buf.size - curstart, s"$id:c$curchap")
              }
            }
            case EvElemStart(_, "div", attr, _) => {
              Try(attr("type").text).getOrElse("") match {
                case "chapter" => {
                  if ( curchap > 0 ) {
                    locs += Locus(curstart, buf.size - curstart, s"$id:c$curchap")
                  }
                  curstart = buf.size
                  curchap += 1
                }
                case _ =>
              }
            }
            case EvText(t) => {
              if ( buffering ) buf ++= t
            }
            case EvEntityRef(n) => {
              if ( buffering ) {
                buf ++= (n match {
                  case "amp" => "&"
                  case "lt" => "<"
                  case "gt" => ">"
                  case "apos" => "'"
                  case "quot" => "\""
                  case _ => ""
                })
              }
            }
            case _ =>
          }
        }
        val bibl = (header \ "fileDesc" \ "sourceDesc" \ "biblFull")
        Some(VACRecord(id, id,
          (header \ "fileDesc" \ "titleStmt" \ "author").text.trim,
          (header \ "fileDesc" \ "titleStmt" \ "title").text.trim,
          (bibl \ "publicationStmt" \ "publisher").text.trim,
          (bibl \ "publicationStmt" \ "pubPlace").text.trim,
          (bibl \ "publicationStmt" \ "date" \ "@when").text.trim,
          buf.toString, locs.toArray))
      })
      .toDF
      .write.save(args(1))
    spark.stop()
  }
}
