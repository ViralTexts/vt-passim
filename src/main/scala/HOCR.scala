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
      .filter(_._1.endsWith(".hocr"))
      .flatMap( in => {
        try {
          val id = in._1.replaceAll(".hocr$", "")
          val buf = new StringBuilder
          val regions = new ArrayBuffer[Region]
          var seq = -1

          val raw = scala.io.Source.fromURL(in._1).mkString

          val tree = scala.xml.XML.loadString(raw.replaceAll("<!DOCTYPE[^>]*>\n?", ""))

          (tree \\ "div")
            .filter(node => node.attribute("class").exists(c => c.text == "ocr_page"))
            .flatMap { page =>
            (page \\ "p") foreach { p =>
              (p \ "span") foreach { line =>
                (line \ "span") foreach { w =>
                  if ( !buf.isEmpty && buf.last != '\n' ) buf ++= " "
                  Try((w \ "@title").text).getOrElse("") match {
                    case bboxPat(l, t, r, b) => {
                      regions += Region(buf.size, w.text.length,
                        Coords(l.toInt, t.toInt,
                          r.toInt - l.toInt, b.toInt - t.toInt, b.toInt - t.toInt))
                    }
                    case _ =>
                  }
                  buf ++= w.text
                }
                if ( !buf.isEmpty ) buf ++= "\n"
              }
              if ( !buf.isEmpty ) buf ++= "\n"
            }
            var width = 0
            var height = 0
            Try((page \ "@title").text).getOrElse("") match {
              case bboxPat(l, t, r, b) =>
                width = r.toInt
                height = b.toInt
              case _ =>
            }
            seq += 1
            Some((id, id, seq, buf.toString, Array(Page(id, seq, width, height, 0, regions.toArray))))
          }
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
