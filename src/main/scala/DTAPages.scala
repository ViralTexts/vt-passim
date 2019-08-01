package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

case class RenditionSpan(rendition: String, start: Int, length: Int)

case class ZoneContent(place: String, data: StringBuilder, rend: ListBuffer[RenditionSpan])

case class Rec(id: String, book: String, seq: Int, place: String, text:String,
  rendition: Array[RenditionSpan])

object DTAPages {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DTAPages Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val fname = new java.io.File(new java.net.URL(in._1).toURI)
        val book = fname.getName.replaceAll("(.TEI-P5)?.xml$", "")
        var seq = -1
        var pageID = ""
        val zoneStack = new Stack[ZoneContent]()
        val rendStack = new Stack[RenditionSpan]()

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, "hi", attr, _) => {
              if ( !zoneStack.isEmpty )
                rendStack.push(RenditionSpan(Try(attr("rendition").text).getOrElse(""),
                  zoneStack.top.data.length, 0))
              Nil
            }
            case EvElemEnd(_, "hi") => {
              if ( !zoneStack.isEmpty ) {
                val start = rendStack.pop
                zoneStack.top.rend ++= start.rendition.split("\\s+")
                  .filter { _ != "" }
                  .map { r => RenditionSpan(r.stripPrefix("#"),
                    start.start, zoneStack.top.data.length - start.start) }
              }
              Nil
            }
            case EvElemStart(_, "pb", attr, _) => {
              val res = new ListBuffer[Rec]()
              if ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                res += Rec(pageID + s"z$seq", book, seq, top.place, top.data.toString, top.rend.toArray)
              }
              // Output printed page number (n attribute not in brackets) here?
              pageID = book + attr("facs").text
              zoneStack.push(new ZoneContent("body", new StringBuilder, new ListBuffer[RenditionSpan]()))
              val pno = Try(attr("n").text).getOrElse("").replaceAll("\\[[^\\]]+\\]", "")
              if ( pno != "" ) {
                seq += 1
                res += Rec(pageID + s"z$seq", book, seq, "page", pno, new Array[RenditionSpan](0))
              }
              res
            }
            case EvElemEnd(_, "text") => {
              if ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                Seq(Rec(pageID + s"z$seq", book, seq, top.place, top.data.toString, top.rend.toArray))
              } else {
                Nil
              }
            }
            case EvElemStart(_, "note", attr, _) => {
              if ( !zoneStack.isEmpty ) {
                zoneStack.push(new ZoneContent(Try(attr("place").text).getOrElse("note"),
                  new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              Nil
            }
            case EvElemEnd(_, "note") => {
              if ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                seq += 1
                Seq(Rec(pageID + s"z$seq", book, seq, top.place, top.data.toString, top.rend.toArray))
              } else
                Nil
            }
            case EvElemStart(_, "fw", attr, _) => {
              if ( !zoneStack.isEmpty ) {
                zoneStack.push(new ZoneContent(Try(attr("place").text).getOrElse("fw"),
                  new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              Nil
            }
            case EvElemEnd(_, "fw") => {
              if ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                seq += 1
                Seq(Rec(pageID + s"z$seq", book, seq, top.place, top.data.toString, top.rend.toArray))
              } else
                Nil
            }
            case EvText(t) => { // remove leading whitespace only if we haven't added anything
              if ( !zoneStack.isEmpty ) zoneStack.top.data ++= (if (zoneStack.top.data.isEmpty) t.replaceAll("^\\s+", "") else t)
              Nil
            }
            case EvEntityRef(n) => {
              if ( !zoneStack.isEmpty ) {
                zoneStack.top.data ++= (n match {
                  case "amp" => "&"
                  case "lt" => "<"
                  case "gt" => ">"
                  case "apos" => "'"
                  case "quot" => "\""
                  case _ => ""
                })
              }
              Nil
            }
            case _ => Nil
          }
        }
      })
      .toDF
      .write.save(args(1))
    spark.stop()
  }
}
