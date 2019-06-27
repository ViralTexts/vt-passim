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
      .flatMap( in => {
        val fname = new java.io.File(new java.net.URL(in._1).toURI)
        val id = fname.getName.replaceAll(".hocr$", "")
        val buf = new StringBuilder
        val regions = new ArrayBuffer[Region]
        var buffering = false
        var seq = -1
        var pageID = ""
        var region = new Region(0, 0, Coords(0,0,0,0,0))

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, "div", attr, _) => {
              Try(attr("class").text).getOrElse("") match {
                case "ocr_page" => {
                  val res = if ( pageID != "" ) {
                    Some((pageID, id, seq, buf.toString, regions.toArray))
                  } else
                    None
                  seq += 1
                  pageID = id + "#" + attr("id").text
                  buf.clear
                  regions.clear
                  res
                }
                case _ => None
              }
            }
            case EvElemEnd(_, "p") => {
              if ( !buf.isEmpty ) buf ++= "\n"
              None
            }
            case EvElemEnd(_, "body") => {
              if ( pageID != "" ) {
                Some((pageID, id, seq, buf.toString, regions.toArray))
              } else {
                None
              }
            }
            case EvElemStart(_, "span", attr, _) => {
              Try(attr("class").text).getOrElse("") match {
                case "ocr_line" => {
                  if ( !buf.isEmpty ) buf ++= "\n"
                }
                case "ocrx_word" => {
                  if ( !buf.isEmpty && buf.last != '\n' ) buf ++= " "
                  buffering = true
                  Try(attr("title").text).getOrElse("") match {
                    case bboxPat(l, t, r, b) => {
                      region = Region(buf.size, 0,
                        Coords(l.toInt, t.toInt,
                          r.toInt - l.toInt, b.toInt - t.toInt, b.toInt - t.toInt))
                    }
                    case _ =>
                  }
                }
                case _ =>
              }
              None
            }
            case EvElemEnd(_, "span") => {
              if ( buffering ) {
                if ( buf.size > region.start ) {
                  regions += region.copy(length = (buf.size - region.start))
                }
                buffering = false
              }
              None
            }
            case EvText(t) => { // remove leading whitespace only if we haven't added anything
              if ( buffering ) buf ++= t.trim
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
      .toDF("id", "book", "seq", "text", "pages")
      .write.save(args(1))
    spark.stop()
  }
}
