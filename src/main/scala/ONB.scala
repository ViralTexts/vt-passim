package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

case class ONBRecord(id: String, issue: String, series: String, seq: Int,
  title: String, date: String, text: String, page_access: String, book_access: String)

object ONB {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ONB records")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val datePat = """(\d{4})(\d\d)(\d\d)$""".r.unanchored

    sc.wholeTextFiles(args(0), sc.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap { f =>
      val t = scala.xml.XML.loadString(f._2)
      val name = t \ "newspaper" \ "name"
      val series = (name \ "@anno_id").text + (name \ "@anno-id").text
      val title = name.text
      (t \ "newspaper" \ "issue").flatMap { issue =>
        val book_access = (issue \ "path").text
        val date = book_access match { case datePat(y,m,d) => s"$y-$m-$d" case _ => "" }
        if ( date == "" ) Nil
        else
          (issue \ "pages" \ "page").map { page =>
            val seqstr = (page \ "number").text match { case "" => "0" case x => x }
            ONBRecord(s"$series/$date/$seqstr", s"$series/$date",
              series, seqstr.toInt, title, date,
              (page \ "text").text.replaceAll("&", "&amp;").replaceAll("<", "&lt;"),
              (page \ "pagePath").text, book_access)
          }
      }
    }
      .toDF
      .write.save(args(1))
  }
}
