package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{coalesce, concat, lit}

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

object APS {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("APS Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val series = sqlContext.read.json(args(1))

    sc.binaryFiles(args(0), sc.defaultParallelism)
      .filter(_._1.endsWith(".zip"))
      .flatMap { x =>
        try {
          val fname = new java.io.File(new java.net.URL(x._1).toURI)
          val issue = fname.getName.replaceAll(".zip$", "")

          val zfile = new java.util.zip.ZipFile(fname)

          zfile.entries
            .filter { _.getName.endsWith(".xml") }
            .flatMap { f =>
            val t = scala.xml.XML.load(zfile.getInputStream(f))
            val series = (t \ "Publication" \ "PublicationID").text
            val sdate = (t \ "NumericPubDate").text
            val text = (t \ "FullText").text.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
              .replaceAll("([ ]{4,})", "\n$1")

            if ( text != "" && series != "" && sdate != "" ) {
              val date = Seq(sdate.substring(0, 4), sdate.substring(4, 6), sdate.substring(6, 8))
                .mkString("-")
              Some(("aps/" + (t \ "RecordID").text,
                "aps/" + series + "/" + date,
                series, date, text,
                (t \ "RecordTitle").text,
                (t \ "ObjectType").text,
                (t \ "URLFullText").text,
                (t \ "LanguageCode").text.toLowerCase,
                (t \ "Contributor" \ "OriginalForm").text))
            } else
              None
          }
        } catch {
          case e: Exception =>
            println(x._1 + e.toString)
            None
        }
      }
      .toDF("id", "issue", "apsseries", "date", "text",
        "heading", "category", "url", "lang", "contributor")
      .join(series, Seq("apsseries"), "left_outer")
      .filter { ('startdate.isNull || 'startdate <= 'date) && ('enddate.isNull || 'enddate >= 'date) }
      .withColumn("series", coalesce('series, concat(lit("aps/"), 'apsseries)))
      .drop("apsseries", "startdate", "enddate", "title")
      .write.save(args(2))
  }
}