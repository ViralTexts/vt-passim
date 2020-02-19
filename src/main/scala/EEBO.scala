package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try
import scala.xml.pull._

import vtpassim.pageinfo._

object EEBO {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("EEBO Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val bboxPat = """(\d+),(\d+),(\d+),(\d+)""".r.unanchored

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(in => in._1.endsWith(".xml") && !in._1.contains("_manifest_"))
      .flatMap( in => {
        try {
          val id = in._1.replaceAll(".xml", "")
          var seq = -1

          val raw = new String(in._2.toArray(), java.nio.charset.StandardCharsets.ISO_8859_1)

          val tree = scala.xml.XML.loadString(raw.replaceAll("<!DOCTYPE[^>]*>\n?", ""))

          val book = (tree \\ "book" \ "bookInfo" \ "documentID").text
          val estcid = (tree \\ "book" \ "bookInfo" \ "ESTCID").text

          (tree \\ "text" \ "page")
            .flatMap { page =>
            val id = (page \ "pageInfo" \ "recordID").text
            val imageLink = (page \ "pageInfo" \ "imageLink").text
            val buf = new StringBuilder
            val regions = new ArrayBuffer[Region]
            (page \ "pageContent" \\ "wd") foreach { w =>
              val c = Try((w \ "@pos").text).getOrElse("") match {
                case bboxPat(l, t, r, b) => {
                  Coords(l.toInt, t.toInt,
                    r.toInt - l.toInt, b.toInt - t.toInt, b.toInt - t.toInt)
                }
                case _ => Coords(-1, -1, -1, -1, -1)
              }
              if ( !buf.isEmpty ) {
                if ( c.x == -1 ) {
                  buf ++= " "
                } else {
                  val prev = regions.last.coords
                  // if ( (c.x < (prev.x + prev.w - 10) ) || ( c.y > (prev.y + prev.h + 5) ) ) {
                  if ( c.x < prev.x ) {
                    buf ++= "\n"
                  } else {
                    buf ++= " "
                  }
                }
              }
              if ( c.x > -1 ) {
                regions += Region(buf.size, w.text.length, c)
              }
              buf ++= w.text
            }
            seq += 1
            Some((id, book, estcid,
              Try(page \ "@type" text).getOrElse(""),
              Try(page \ "@firstPage" text).getOrElse("no"),
              Try(page \ "pageInfo" \ "sourcePage" text).getOrElse(null),
              seq, buf.toString, Array(Page(imageLink, seq, 0, 0, 0, regions.toArray))))
          }
        } catch {
          case ex: Exception =>
            Console.err.println("## " + in._1 + ": " + ex.toString)
            None
        }
      })
      .toDF("id", "book", "estcid", "type", "firstPage", "sourcePage", "seq", "text", "pages")
      .withColumn("firstPage", $"firstPage".cast("boolean"))
      .write.save(args(1))
    spark.stop()
  }
}
