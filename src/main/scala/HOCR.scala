package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try
import scala.xml.pull._

import vtpassim.pageinfo._

object HOCR {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("hOCR Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val bboxPat = """bbox\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)""".r.unanchored

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(f => f._1.endsWith(".hocr") || f._1.endsWith(".html"))
      .flatMap( in => {
        try {
          val book = in._1.replaceAll(".hocr$", "")
          val buf = new StringBuilder
          val regions = new ArrayBuffer[Region]
          var id = ""
          var seq = -1
          var buffering = false

          val raw = scala.io.Source.fromURL(in._1).mkString
          val clean = raw.replaceAll("<!DOCTYPE[^>]*>\n?", "")
          val pass = new XMLEventReader(scala.io.Source.fromString(clean))
          pass.flatMap { event =>
            event match {
              case EvElemEnd(_, "html") => {
                if ( !buf.isEmpty ) {
                  Some((id, book, seq, buf.toString, ""))
                } else {
                  None
                }
              }
              case EvElemStart(_, "div", attr, _) => {
                if ( Try(attr("class").text).getOrElse("") == "ocr_page" ) {
                  val res = if ( !buf.isEmpty ) {
                    Some((id, book, seq, buf.toString, ""))
                  } else {
                    None
                  }
                  id = book + Try("#" + attr("id").text).getOrElse("")
                  buffering = false
                  buf.clear
                  regions.clear
                  seq += 1
                  res
                } else {
                  None
                }
              }
              case EvElemStart(_, "span", attr, _) => {
                Try(attr("class").text).getOrElse("") match {
                  case "ocr_line" => {
                    if ( !buf.isEmpty ) buf ++= "\n"
                    None
                  }
                  case "ocrx_word" => {
                    buffering = true
                    if ( !buf.isEmpty && !buf.last.isWhitespace ) buf += ' '
                    None
                  }
                }
              }
              case EvElemEnd(_, "span") => {
                buffering = false
                None
              }
              case EvElemEnd(_, "p") => {
                if ( !buf.isEmpty ) buf ++= "\n\n"
                None
              }
              case EvText(t) => {
                if ( buffering && t != " " ) buf ++= t
                None
              }
              case EvEntityRef(n) => {
                if ( buffering ) buf ++= (n match {
                  case "amp" => "&"
                  case "lt" => "<"
                  case "gt" => ">"
                  case "apos" => "'"
                  case "quot" => "\""
                  case _ => ""
                })
                None
              }
              case _ => None
            }
          }

          // (tree \\ "div")
          //   .filter(node => node.attribute("class").exists(c => c.text == "ocr_page"))
          //   .flatMap { page =>
          //   val id = book + Try("#" + (page \ "@id").text).getOrElse("")
          //   val buf = new StringBuilder
          //   val regions = new ArrayBuffer[Region]
          //   (page \\ "p") foreach { p =>
          //     (p \ "span") foreach { line =>
          //       (line \ "span") foreach { w =>
          //         if ( !buf.isEmpty && buf.last != '\n' ) buf ++= " "
          //         Try((w \ "@title").text).getOrElse("") match {
          //           case bboxPat(l, t, r, b) => {
          //             regions += Region(buf.size, w.text.length,
          //               Coords(l.toInt, t.toInt,
          //                 r.toInt - l.toInt, b.toInt - t.toInt, b.toInt - t.toInt))
          //           }
          //           case _ =>
          //         }
          //         buf ++= w.text
          //       }
          //       if ( !buf.isEmpty ) buf ++= "\n"
          //     }
          //     if ( !buf.isEmpty ) buf ++= "\n"
          //   }
          //   var width = 0
          //   var height = 0
          //   Try((page \ "@title").text).getOrElse("") match {
          //     case bboxPat(l, t, r, b) =>
          //       width = r.toInt
          //       height = b.toInt
          //     case _ =>
          //   }
          //   seq += 1
          //   Some((id, book, seq, buf.toString, Array(Page(id, seq, width, height, 0, regions.toArray))))
        } catch {
          case ex: Exception =>
            Console.err.println("## " + in._1 + ": " + ex.toString)
            None
        }
      })
      .toDF("id", "book", "seq", "text", "pages")
      .write.save(args(1))
    spark.stop()
  }
}
