package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

case class RenditionSpan(rendition: String, start: Int, length: Int)

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
        val buf = new StringBuilder
        var buffering = false
        var seq = -1
        var pageID = ""
        val rendStack = new Stack[RenditionSpan]()
        val rendSpans = new ListBuffer[RenditionSpan]()

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, "hi", attr, _) => {
              if ( buffering )
                rendStack.push(RenditionSpan(attr("rendition").text, buf.length, 0))
              None
            }
            case EvElemEnd(_, "hi") => {
              if ( buffering ) {
                val start = rendStack.pop
                rendSpans ++= start.rendition.split("\\s+")
                  .map { r => RenditionSpan(r.stripPrefix("#"),
                    start.start, buf.length - start.start) }
              }
              None
            }
            case EvElemStart(_, "pb", attr, _) => {
              val rec = if ( buffering ) {
                Some((pageID, id, seq, buf.toString, rendSpans.toArray))
              } else {
                None
              }
              pageID = id + attr("facs").text
              buffering = true
              seq += 1
              buf.clear
              rendSpans.clear
              rec
            }
            case EvElemEnd(_, "text") => {
              if ( buffering ) {
                seq += 1
                val text = buf.toString
                buffering = false
                buf.clear
                rendSpans.clear
                Some((pageID, id, seq, text, rendSpans.toArray))
              } else {
                None
              }
            }
            case EvElemStart(_, "note", attr, _) => {
              buffering = false
              None
            }
            case EvElemEnd(_, "note") => {
              if ( !buf.isEmpty ) buffering = true
              None
            }
            case EvText(t) => { // remove leading whitespace only if we haven't added anything
              if ( buffering ) buf ++= (if (buf.isEmpty) t.replaceAll("^\\s+", "") else t)
              None
            }
            case EvEntityRef(n) => {
              if ( buffering ) buf ++= "&" + n + ";"
              None
            }
            case _ => None
          }
        }
      })
      .toDF("id", "book", "seq", "text", "rendition")
      .write.save(args(1))
    spark.stop()
  }
}
