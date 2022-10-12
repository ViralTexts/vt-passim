package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, broadcast, concat_ws, sort_array, collect_set, posexplode, size}
import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import vtpassim.pageinfo._

case class NCNPRecord(id: String, issue: String, series: String, seq: Int,
  title: String, date: String,
  altSource: String, category: String, text: String, pages: Array[Page])

object NCNP {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("NCNP records").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val seriesMap = spark.read.json(args(1))

    val raw = spark.sparkContext.wholeTextFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap { f =>
      try {
        val t = scala.xml.XML.loadString(f._2.replaceFirst("<!DOCTYPE[^>]*>", ""))
        val issue = (t \ "id").text
        val series =
          if ( (t \ "lccn").size > 0 ) {
            val raw = (t \ "lccn").text
            if ( raw.startsWith("NOLC") )
              raw
            else if ( raw.size == 8 )
              "/lccn/sn" + raw
            else
              "/lccn/" + raw
          } else {
            (t \ "titleAbbreviation").text
          }
        val date = (t \ "pf").text.replaceAll("""^(\d\d\d\d)(\d\d)(\d\d).*$""", "$1-$2-$3")

        // First pass to determine page sizes
        val pdim =
          (t \ "article" \ "text" \ "_" \ "pg").map { clip =>
            val pn = (clip \ "@pgref").text.toInt
            val c = (clip \ "@pos").text.split(",").map(_.toInt)
            (pn, c(2), c(3))
          }
            .groupBy { _._1 }
            .mapValues { z => (z.map(_._2).reduce(Math.max), z.map(_._3).reduce(Math.max)) }

        val pid = (t \ "article" \ "pi").map(p => ((p \ "@pgref").text.toInt, p.text)).toMap

        (t \ "article").zipWithIndex.map { case (article, seq) =>
          val buf = new StringBuilder
          val pages = new ArrayBuffer[Page]
          val regions = new ArrayBuffer[Region]
          var lastX = -1
          var lastY = -20
          var cur = -1
            (article \ "text" \ "_").foreach { clip =>
              val pn = (clip \ "pg" \ "@pgref").text.toInt
              if ( pn != cur ) {
                if ( cur != -1 )
                  pages += Page(pid(cur), cur, pdim(cur)._1, pdim(cur)._2, 0, regions.toArray)
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
            pages += Page(pid(cur), cur, pdim(cur)._1, pdim(cur)._2, 0, regions.toArray)
          NCNPRecord((article \ "id").text, issue, series, seq, (article \ "ti").text, date,
            (article \ "altSource").text, (article \ "ct").text,
            buf.toString, pages.toArray)
        }
      } catch {
        case e: Exception =>
          println(f._1 + e.toString)
          None
      }
    }
      .toDF
      .join(seriesMap, Seq("series"), "left_outer")
      .withColumn("series", coalesce('correct_series, 'series))
      .drop("correct_series")
      .withColumn("issue", concat_ws("/", 'series, 'issue))
      .withColumn("id", concat_ws("/", 'series, 'id))
      .dropDuplicates("id")

    val dupIssues = raw.groupBy("series", "date")
      .agg(sort_array(collect_set("issue")) as "issues")
      .filter(size('issues) > 1)
      .select(posexplode('issues))
      .filter('pos > 0)
      .select('col as "issue")

    dupIssues.cache()

    println("# duplicate issues: " + dupIssues.count)

    raw.join(broadcast(dupIssues), Seq("issue"), "left_anti")
      .write.mode("overwrite").save(args(2))
    spark.stop()
  }
}
