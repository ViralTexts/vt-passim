package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import vtpassim.pageinfo._

case class NCNPRecord(id: String, issue: String, series: String, seq: Int,
  title: String, date: String,
  heading: String, altSource: String, category: String, text: String, regions: Array[Region])

object NCNP {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NCNP records")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.wholeTextFiles(args(0), sc.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap { f =>
      try {
        val t = scala.xml.XML.loadString(f._2.replaceFirst("<!DOCTYPE[^>]*>", ""))
        val issue = (t \ "id").text
        val series =
          if ( (t \ "lccn").size > 0 ) {
            "sn" + (t \ "lccn").text
          } else {
            (t \ "titleAbbreviation").text
          }
        val title = (t \ "jn").text
        val date = (t \ "pf").text.replaceAll("""^(\d\d\d\d)(\d\d)(\d\d).*$""", "$1-$2-$3")
        (t \ "article").zipWithIndex.map { case (article, seq) =>
          val buf = new StringBuilder
          val regions = new ArrayBuffer[Region]
          var lastX = -1
          var lastY = -20
            (article \\ "wd").foreach { w =>
              val start = buf.size
              val c = (w \ "@pos").text.split(",").map(_.toInt)
              val sep = if ( lastX < 0 || lastY < 0 ) {
                ""
              } else if ( c(0) < lastX || c(1) > (lastY + 20) ) {
                "\n"
              } else " "
              lastX = c(0)
              lastY = c(1)
              buf.append(sep + w.text.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
                .replaceFirst("""(\S)-$""", "$1\u00ad"))
              regions += Region(start, buf.size - start,
                Coords(c(0), c(1), c(2) - c(0), c(3) - c(1), c(3) - c(1)))
            }
          NCNPRecord((article \ "id").text, issue, series, seq, title, date,
            (article \ "ti").text, (article \ "altSource").text, (article \ "ct").text,
            buf.toString, regions.toArray)
        }
      } catch {
        case e: Exception =>
          println(f._1 + e.toString)
          None
      }
    }
      .toDF
      .write.save(args(1))
  }
}
