package vtpassim

import org.apache.spark.sql.SparkSession

case class ONBRecord(id: String, issue: String, series: String, seq: Int,
  date: String, text: String, page_access: String, book_access: String)

object ONB {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ONB import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val datePat = """(\d{4})(\d\d)(\d\d)$""".r.unanchored

    spark.sparkContext.wholeTextFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap { f =>
      val t = scala.xml.XML.loadString(f._2)
      val name = t \ "newspaper" \ "name"
      val rawSeries = (name \ "@anno_id").text + (name \ "@anno-id").text
      val series = (if (rawSeries == "aid") "dea" else rawSeries)
      (t \ "newspaper" \ "issue").flatMap { issue =>
        val book_access = (issue \ "path").text
        val date = book_access match { case datePat(y,m,d) => s"$y-$m-$d" case _ => "" }
        if ( date == "" ) Nil
        else
          (issue \ "pages" \ "page").map { page =>
            val seqstr = (page \ "number").text match { case "" => "0" case x => x }
            ONBRecord(s"$series/$date/$seqstr", s"$series/$date",
              series, seqstr.toInt, date,
              (page \ "text").text,
              (page \ "pagePath").text, book_access)
          }
      }
    }
      .toDF
      .dropDuplicates("id")
      .write.mode("overwrite").save(args(1))
    spark.stop()
  }
}
