package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Map, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

case class TRec(id: String, book: String, seq: Int, page: String, imageid: String,
  ztype: String, place: String,
  text:String, rendition: Array[RenditionSpan])

object TCPPages {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TCPPages Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val letters = "abcdefghijklmnopqrstuvwxyz"

    val lines = Seq("head")
    val floats = Seq("figure", "note", "table")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val fname = new java.io.File(new java.net.URL(in._1).toURI)
        val book = fname.getName.replaceAll("(.TEI-P5)?.xml$", "")
        var seq = -1
        var pageID = ""
        var imageID = ""
        val zoneStack = new Stack[ZoneContent]()
        val rendStack = new Stack[RenditionSpan]()
        val pageCount = Map[String, Int]()

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, elem, attr, _) if lines.contains(elem) && !zoneStack.isEmpty => {
              rendStack.push(RenditionSpan(elem, zoneStack.top.data.length, 0))
              Nil
            }
            case EvElemEnd(_, elem) if lines.contains(elem) && !zoneStack.isEmpty && !rendStack.isEmpty => {
              val start = rendStack.pop
              zoneStack.top.rend += RenditionSpan(start.rendition, start.start,
                zoneStack.top.data.length - start.start)
              Nil
            }
            case EvElemStart(_, "hi", attr, _) if !zoneStack.isEmpty => {
              rendStack.push(RenditionSpan(Try(attr("rendition").text).getOrElse(""),
                zoneStack.top.data.length, 0))
              Nil
            }
            case EvElemEnd(_, "hi") if !zoneStack.isEmpty && !rendStack.isEmpty => {
              val start = rendStack.pop
              zoneStack.top.rend ++= start.rendition.split("\\s+")
                .filter { _ != "" }
                .map { r => RenditionSpan(r.stripPrefix("#"),
                  start.start, zoneStack.top.data.length - start.start) }
              Nil
            }
            case EvElemStart(_, "pb", attr, _) => {
              // Record and restart all open rendition spans
              for ( i <- 0 until rendStack.length ) {
                zoneStack.top.rend += RenditionSpan(rendStack(i).rendition, rendStack(i).start,
                  zoneStack.top.data.length - rendStack(i).start)
                rendStack(i) = RenditionSpan(rendStack(i).rendition, 0, 0)
              }
              val res = new ListBuffer[TRec]()
              // Remember and output all open zones
              val zones = (if ( zoneStack.isEmpty ) Seq(ZoneInfo("body", "body")) else zoneStack.toSeq.map(_.info).reverse)
              while ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                res += TRec(pageID + s"z$seq", book, seq, pageID, imageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray)
              }
              for ( zone <- zones ) {
                zoneStack.push(new ZoneContent(zone, new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              imageID = attr("facs").text
              if ( pageCount.contains(imageID) ) {
                pageCount(imageID) += 1
              } else {
                pageCount(imageID) = 1
              }
              val suff = letters(pageCount(imageID) - 1).toString
              val iseq = Try(imageID.split("[:_]")(2).toInt).getOrElse(seq)
              pageID = f"$book%s-$iseq%03d-$suff%s"
              val pno = Try(attr("n").text).getOrElse("").replaceAll("\\[[^\\]]+\\]", "")
              // Output printed page number (n attribute not in brackets) here
              if ( pno != "" ) {
                seq += 1
                res += TRec(pageID + s"z$seq", book, seq, pageID, imageID, "pageNum", "pageNum", pno, new Array[RenditionSpan](0))
              }
              res
            }
            case EvElemStart(_, "cb", attr, _) => {
              // Record and restart all open rendition spans
              for ( i <- 0 until rendStack.length ) {
                zoneStack.top.rend += RenditionSpan(rendStack(i).rendition, rendStack(i).start,
                  zoneStack.top.data.length - rendStack(i).start)
                rendStack(i) = RenditionSpan(rendStack(i).rendition, 0, 0)
              }
              val res = new ListBuffer[TRec]()
              // Remember and output all open zones
              val zones = (if ( zoneStack.isEmpty ) Seq(ZoneInfo("body", "body")) else zoneStack.toSeq.map(_.info).reverse)
              while ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                if ( top.data.toString != "" ) {
                  seq += 1
                  res += TRec(pageID + s"z$seq", book, seq, pageID, imageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray)
                }
              }
              for ( zone <- zones ) {
                zoneStack.push(new ZoneContent(zone, new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              // Output printed column number (n attribute not in brackets) here?
              val cno = Try(attr("n").text).getOrElse("").replaceAll("\\[[^\\]]+\\]", "")
              if ( cno != "" ) {
                seq += 1
                res += TRec(pageID + s"z$seq", book, seq, pageID, imageID, "colNum", "colNum", cno, new Array[RenditionSpan](0))
              }
              res
            }
            case EvElemEnd(_, "text") => {
              val res = new ListBuffer[TRec]()
              while ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                res += TRec(pageID + s"z$seq", book, seq, pageID, imageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray)
              }
              res
            }
            case EvElemStart(_, elem, attr, _) if floats.contains(elem) && !zoneStack.isEmpty => {
              zoneStack.top.rend += RenditionSpan(elem, zoneStack.top.data.length, 0)
              zoneStack.push(new ZoneContent(ZoneInfo(elem,
                Try(attr("place").text).getOrElse(elem)),
                new StringBuilder, new ListBuffer[RenditionSpan]()))
              Nil
            }
            case EvElemEnd(_, elem) if floats.contains(elem) && !zoneStack.isEmpty => {
              val top = zoneStack.pop
              seq += 1
              Seq(TRec(pageID + s"z$seq", book, seq, pageID, imageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray))
            }
            case EvElemStart(_, "fw", attr, _) if !zoneStack.isEmpty => {
              zoneStack.push(new ZoneContent(ZoneInfo(Try(attr("type").text).getOrElse("fw"),
                Try(attr("place").text).getOrElse("fw")),
                new StringBuilder, new ListBuffer[RenditionSpan]()))
              Nil
            }
            case EvElemEnd(_, "fw") if !zoneStack.isEmpty => {
              val top = zoneStack.pop
              seq += 1
              Seq(TRec(pageID + s"z$seq", book, seq, pageID, imageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray))
            }
            case EvElemEnd(_, "cell") if !zoneStack.isEmpty => {
              zoneStack.top.data ++= "\t"
              Nil
            }
            case EvElemStart(_, "gap", attr, _) if !zoneStack.isEmpty => {
              zoneStack.push(new ZoneContent(ZoneInfo("quash", "quash"),
                new StringBuilder, new ListBuffer[RenditionSpan]()))
              Nil
            }
            case EvElemEnd(_, "gap") if !zoneStack.isEmpty => {
              val top = zoneStack.pop
              zoneStack.top.data ++= top.data.toString.replaceAll("^\\s+", "").replaceAll("\\s+$", "").replaceAll("^.*non-Latin.*$", "@@@")
              Nil
            }
            case EvElemStart(_, "g", attr, _) if !zoneStack.isEmpty => {
              if ( attr.get("ref").map(_.text).headOption.getOrElse("") == "char:EOLhyphen" ) {
                zoneStack.top.data ++= "\u00ad\n"
              }
              Nil
            }
            case EvText(t) if !zoneStack.isEmpty => {
              // remove leading whitespace only if we haven't added anything
              zoneStack.top.data ++= (if (zoneStack.top.data.isEmpty) t.replaceAll("^\\s+", "") else t).replaceAll("\\n[ ]+", "\n")
              Nil
            }
            case EvEntityRef(n) if !zoneStack.isEmpty => {
              zoneStack.top.data ++= (n match {
                case "amp" => "&"
                case "lt" => "<"
                case "gt" => ">"
                case "apos" => "'"
                case "quot" => "\""
                case _ => ""
              })
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
