package vtpassim

import org.apache.spark.sql.SparkSession

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import vtpassim.pageinfo._

case class NCCOIssueRecord(id: String, issue: String, series: String, seq: Int, date: String,
  title: String, altSource: String, category: String, text: String, pages: Array[IIIFPage])

object NCCOIssue {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("NCCO records").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.wholeTextFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap { f =>
      try {
        val t = scala.xml.XML.loadString(f._2.replaceFirst("<!DOCTYPE[^>]*>", ""))
        val meta = (t \ "metadatainfo")
        val issue = (meta \ "PSMID").text
        val series = (meta \ "newspaperID").text
        val date = (meta \ "da" \ "searchableDateStart").text
          .replaceAll("""^(\d\d\d\d)(\d\d)(\d\d).*$""", "$1-$2-$3")

        // First pass to determine page sizes
        val pdim = (t \ "page" \ "pageid")
          .map { page =>
          (page.text, ((page \ "@width").text.toInt, (page \ "@height").text.toInt))
        }
          .toMap

        (t \ "page" \ "article").zipWithIndex.map { case (article, seq) =>
          val pid = (article \ "pi")
            .map { pid => ((pid \ "@pgref").text.toInt, pid.text) }
            .toMap
          val buf = new StringBuilder
          val pages = new ArrayBuffer[IIIFPage]
          val regions = new ArrayBuffer[Region]
          var lastX = -1
          var lastY = -20
          var cur = -1
            (article \ "text" \ "_").foreach { clip =>
              val pn = (clip \ "pg" \ "@pgref").text.toInt
              if ( pn != cur ) {
                if ( cur != -1 )
                  pages += IIIFPage(pid(cur), null, cur, pdim(pid(cur))._1, pdim(pid(cur))._2, 0, regions.toArray)
                regions.clear
                cur = pn
              }
                (clip \\ "wd").foreach { w =>
                  val start = buf.size
                  val c = (w \ "@pos").text.split(",").map(_.toInt)
                  val sep = if ( lastX < 0 || lastY < 0 ) {
                    ""
                  } else if ( c(0) < lastX || c(1) > (lastY + 20) ) {
                    "\n"
                  } else " "
                  lastX = c(0)
                  lastY = c(1)
                  buf.append(sep + w.text.replaceFirst("""(\S)-$""", "$1\u00ad"))
                  regions += Region(start, buf.size - start,
                    Coords(c(0), c(1), c(2) - c(0), c(3) - c(1), c(3) - c(1)))
                }
            }
          if ( cur != -1 )
            pages += IIIFPage(pid(cur), null, cur, pdim(pid(cur))._1, pdim(pid(cur))._2, 0, regions.toArray)
          NCCOIssueRecord((article \ "id").text, issue, series, seq, date,
            (article \ "ti").text, (article \ "altSource").text, (article \ "ct").text,
            buf.toString, pages.toArray)
        }
      } catch {
        case e: Exception =>
          println(f._1 + e.toString)
          None
      }
    }
      .toDF
      .write.mode("overwrite").save(args(1))
    spark.stop()
  }
}
