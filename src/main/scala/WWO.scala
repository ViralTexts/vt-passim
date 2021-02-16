package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Map, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

object WWO {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("WWO Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val lines = Seq("head")
    val floats = Seq("figure", "table")
    val quash = Seq("corr", "figDesc")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val fname = new java.io.File(new java.net.URL(in._1).toURI)
        val book = fname.getName.replaceAll("(.TEI-P5)?.xml$", "")
        var seq = -1
        var pseq = 0
        var pageID = book
        val zoneStack = new Stack[ZoneContent]()
        val rendStack = new Stack[RenditionSpan]()
        val id2p = Map[String,String]()

        val stream = new XMLEventReader(scala.io.Source.fromURL(in._1))
        stream.foreach { event =>
          event match {
            case EvElemStart(_, "pb", attr, _) => {
              pseq += 1
              pageID = book + "#" + Try(attr.asAttrMap("xml:id")).getOrElse("p" + pseq)
            }
            case EvElemStart(_, "anchor", attr, _) => {
              val aid = Try(attr.asAttrMap("xml:id")).getOrElse("")
              if ( aid != "" ) {
                id2p("#" + aid) = pageID
              }
            }
            case _ =>
          }
        }

        pseq = 0

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, elem, attr, _) if lines.contains(elem) && !zoneStack.isEmpty => {
              rendStack.push(RenditionSpan(elem, zoneStack.top.data.length, 0))
              Nil
            }
            case EvElemEnd(_, elem) if lines.contains(elem) && !zoneStack.isEmpty => {
              zoneStack.top.data ++= "\n"
              val start = rendStack.pop
              zoneStack.top.rend += RenditionSpan(start.rendition, start.start,
                zoneStack.top.data.length - start.start)
              Nil
            }
            case EvElemStart(_, elem, attr, _) if quash.contains(elem) && !zoneStack.isEmpty => {
              zoneStack.push(new ZoneContent(ZoneInfo("quash", "quash"),
                new StringBuilder, new ListBuffer[RenditionSpan]()))
              Nil
            }
            case EvElemEnd(_, elem) if quash.contains(elem) && !zoneStack.isEmpty => {
              zoneStack.pop
              Nil
            }
            case EvElemStart(_, "hi", attr, _) if !zoneStack.isEmpty => {
              rendStack.push(RenditionSpan(Try(attr("rendition").text).getOrElse(""),
                zoneStack.top.data.length, 0))
              Nil
            }
            case EvElemEnd(_, "hi") if !zoneStack.isEmpty => {
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
              val res = new ListBuffer[Rec]()
              // Remember and output all open zones
              val zones = (if ( zoneStack.isEmpty ) Seq(ZoneInfo("body", "body")) else zoneStack.toSeq.map(_.info).reverse)
              while ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                res += Rec(pageID + s"z$seq", book, seq, pageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray)
              }
              for ( zone <- zones ) {
                zoneStack.push(new ZoneContent(zone, new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              pseq += 1
              val pid = Try(attr.asAttrMap("xml:id")).getOrElse("p" + pseq)
              pageID = book + "#" + pid
              res
            }
            case EvElemStart(_, "cb", attr, _) => {
              // Record and restart all open rendition spans
              for ( i <- 0 until rendStack.length ) {
                zoneStack.top.rend += RenditionSpan(rendStack(i).rendition, rendStack(i).start,
                  zoneStack.top.data.length - rendStack(i).start)
                rendStack(i) = RenditionSpan(rendStack(i).rendition, 0, 0)
              }
              val res = new ListBuffer[Rec]()
              // Remember and output all open zones
              val zones = (if ( zoneStack.isEmpty ) Seq(ZoneInfo("body", "body")) else zoneStack.toSeq.map(_.info).reverse)
              while ( !zoneStack.isEmpty ) {
                val top = zoneStack.pop
                if ( top.data.toString != "" ) {
                  seq += 1
                  res += Rec(pageID + s"z$seq", book, seq, pageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray)
                }
              }
              for ( zone <- zones ) {
                zoneStack.push(new ZoneContent(zone, new StringBuilder, new ListBuffer[RenditionSpan]()))
              }
              // Output printed column number (n attribute not in brackets) here?
              val cno = Try(attr("n").text).getOrElse("").replaceAll("\\[[^\\]]+\\]", "")
              if ( cno != "" ) {
                seq += 1
                res += Rec(pageID + s"z$seq", book, seq, pageID, "colNum", "colNum", cno, new Array[RenditionSpan](0))
              }
              res
            }
            case EvElemEnd(_, "text") => {
              val res = new ListBuffer[Rec]()
              while ( !zoneStack.isEmpty ) {
                seq += 1
                val top = zoneStack.pop
                res += Rec(pageID + s"z$seq", book, seq, pageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray)
              }
              res
            }
            case EvElemStart(_, "notes", attr, _) => {
              zoneStack.push(new ZoneContent(ZoneInfo("notes", "notes"), new StringBuilder, new ListBuffer[RenditionSpan]()))
              Nil
            }
            case EvElemEnd(_, "notes") => {
              zoneStack.pop
              Nil
            }
            case EvElemStart(_, "note", attr, _) if !zoneStack.isEmpty => {
              val rend = Try(attr("rend").text).getOrElse("note")
              zoneStack.push(new ZoneContent(ZoneInfo("note",
                Try("""place\(([^)]+)\)""".r.findFirstMatchIn(rend).head.group(1))
                  .getOrElse("note")),
                new StringBuilder, new ListBuffer[RenditionSpan]()))
              val pid = Try(id2p(attr("target").text)).getOrElse("")
              if ( pid != "" ) {
                pageID = pid
              }
              zoneStack.top.data ++= Try("""pre\(([^)]+)\)""".r.findFirstMatchIn(rend)
                .head.group(1)).getOrElse("")
              Nil
            }
            case EvElemEnd(_, "note") if !zoneStack.isEmpty => {
              val top = zoneStack.pop
              seq += 1
              Seq(Rec(pageID + s"z$seq", book, seq, pageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray))
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
              Seq(Rec(pageID + s"z$seq", book, seq, pageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray))
            }
            case EvElemStart(_, "mw", attr, _) if !zoneStack.isEmpty => {
              val mtype = Try(attr("type").text).getOrElse("fw")
              val rend = Try(attr("rend").text).getOrElse("")
              zoneStack.push(new ZoneContent(ZoneInfo(mtype,
                Try("""align\(([^)]+)\)""".r.findFirstMatchIn(rend).head.group(1))
                  .getOrElse(mtype)),
                new StringBuilder, new ListBuffer[RenditionSpan]()))
              Nil
            }
            case EvElemEnd(_, "mw") if !zoneStack.isEmpty => {
              val top = zoneStack.pop
              seq += 1
              Seq(Rec(pageID + s"z$seq", book, seq, pageID, top.info.ztype, top.info.place, top.data.toString, top.rend.toArray))
            }
            case EvElemEnd(_, "cell") if !zoneStack.isEmpty => {
              zoneStack.top.data ++= "\t"
              Nil
            }
            case EvElemStart(_, "lb", attr, _) if !zoneStack.isEmpty => {
              zoneStack.top.data ++= "\n"
              Nil
            }
            case EvElemEnd(_, "l") if !zoneStack.isEmpty => {
              zoneStack.top.data ++= "\n"
              Nil
            }
            case EvText(t) if !zoneStack.isEmpty => {
              // remove leading whitespace only if we haven't added anything
              zoneStack.top.data ++= (if (zoneStack.top.data.isEmpty) t.replaceAll("^\\s+", "") else t).replaceAll("\\n+\\s*", "")
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
