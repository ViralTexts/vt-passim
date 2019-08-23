package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, concat, lit}
import collection.JavaConversions._

object APSMeta {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("APS Application").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val seriesMap = spark.read.json(args(1))

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
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
            val sdate = (t \ "NumericPubDate").text
            val pub = (t \ "Publication")
            val series = (pub \ "PublicationID").text

            if ( series != "" && sdate != "") {
              val date = Seq(sdate.substring(0, 4), sdate.substring(4, 6), sdate.substring(6, 8))
                .mkString("-")
              Some((series, date,
                (pub \ "Title").text,
                (pub \ "Qualifier").text))
            } else
              None
          }
        } catch {
          case e: Exception =>
            println(x._1 + e.toString)
            None
        }
      }
      .toDF("apsseries", "date", "title", "placeOfPublication")
      .join(seriesMap.drop("title"), Seq("apsseries"), "left_outer")
      .filter { ('startdate.isNull || 'startdate <= 'date) && ('enddate.isNull || 'enddate >= 'date) }
      .withColumn("series", coalesce('series, concat(lit("aps/"), 'apsseries)))
      .drop("apsseries", "startdate", "enddate", "date")
      .distinct
      .write.json(args(2))

    spark.stop()
  }
}
