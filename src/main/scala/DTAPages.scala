package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

case class RenditionSpan(rendition: String, start: Int, length: Int)

case class ZoneContent(place: String, data: StringBuilder, rend: ListBuffer[RenditionSpan])

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
        val id = fname.getName.replaceAll("(.TEI-P5)?.xml$", "")
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
              None
            }
            case EvElemEnd(_, "hi") => {
              if ( !zoneStack.isEmpty ) {
                val start = rendStack.pop
                zoneStack.top.rend ++= start.rendition.split("\\s+")
                  .filter { _ != "" }
                  .map { r => RenditionSpan(r.stripPrefix("#"),
                    start.start, zoneStack.top.data.length - start.start) }
              }
              None
            }
            case EvElemStart(_, "pb", attr, _) => {
              val rec = if ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                Some((pageID, id, seq, top.place, top.data.toString, top.rend.toArray))
              } else {
                None
              }
              pageID = id + attr("facs").text
              seq += 1
              zoneStack.push(new ZoneContent("page", new StringBuilder, new ListBuffer[RenditionSpan]()))
              rec
            }
            case EvElemEnd(_, "text") => {
              if ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                Some((pageID, id, seq, top.place, top.data.toString, top.rend.toArray))
              } else {
                None
              }
            }
            case EvElemStart(_, "note", attr, _) => {
              if ( !zoneStack.isEmpty ) {
                zoneStack.push(new ZoneContent(Try(attr("place").text).getOrElse("note"),
                  new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              None
            }
            case EvElemEnd(_, "note") => {
              if ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                Some((pageID, id, seq, top.place, top.data.toString, top.rend.toArray))
              } else
                None
            }
            case EvElemStart(_, "fw", attr, _) => {
              if ( !zoneStack.isEmpty ) {
                zoneStack.push(new ZoneContent(Try(attr("place").text).getOrElse("fw"),
                  new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              None
            }
            case EvElemEnd(_, "fw") => {
              if ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                Some((pageID, id, seq, top.place, top.data.toString, top.rend.toArray))
              } else
                None
            }
            case EvText(t) => { // remove leading whitespace only if we haven't added anything
              if ( !zoneStack.isEmpty ) zoneStack.top.data ++= (if (zoneStack.top.data.isEmpty) t.replaceAll("^\\s+", "") else t)
              None
            }
            case EvEntityRef(n) => {
              if ( !zoneStack.isEmpty ) zoneStack.top.data ++= "&" + n + ";"
              None
            }
            case _ => None
          }
        }
      })
      .toDF("id", "book", "seq", "place", "text", "rendition")
      .write.save(args(1))
    spark.stop()
  }
}
