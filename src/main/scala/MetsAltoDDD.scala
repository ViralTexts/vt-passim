package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, regexp_extract, udf}
import collection.JavaConversions._
import scala.util.Try
import vtpassim.pageinfo._

case class AltoInfo(text: String, pages: Array[Page])

case class MetaInfo(issue: String, series: String, date: String)

object MetsAltoJoined {
  val parsePage = udf { (s: String, id: String) =>
    try {
      val t = scala.xml.XML.loadString(s)
      val page = (t \ "Layout" \ "Page")
      val seq = Try((page \ "@PHYSICAL_IMG_NR").text.toInt).getOrElse(0)
      val width = Try((page \ "@WIDTH").text.toInt).getOrElse(0)
      val height = Try((page \ "@HEIGHT").text.toInt).getOrElse(0)
      val (text, regions) = MetsAlto.altoText(t)
      AltoInfo(text, Array(Page(id, seq, width, height, 0, regions)))
    } catch {
      case e: Exception => {
        println(e.toString)
        AltoInfo(null, Array[Page]())
      }
    }
  }

  val parseMeta = udf { (s: String) =>
    try {
      val t = scala.xml.XML.loadString(s)
      val issue = (t \ "GetRecord" \ "record" \ "header" \ "identifier").text
      val meta = (t \\ "dcx").head
      val series = "DDD:PPN:" + (meta \ "identifier").head.text
      val date = (meta \ "date").head.text
      MetaInfo(issue, series, date)
    } catch {
      case e: Exception => {
        println(e.toString)
        MetaInfo(null, null, null)
      }
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("MetsAlto import").getOrCreate()
    import spark.implicits._

    val meta = spark.read.json(args(0))
      .withColumn("meta_info", parseMeta('xml))
      .select($"meta_info.*")
      .filter('issue.isNotNull)

    val raw = spark.read.json(args(1))

    spark.conf.set("spark.sql.shuffle.partitions", raw.rdd.getNumPartitions)

    raw.withColumn("id", regexp_extract('url, "urn=(ddd:.+):alto", 1))
      .withColumn("page_info", parsePage('xml, 'id))
      .select('id, 'identifier as "issue", 'url as "page_access", $"page_info.*")
      .withColumn("seq", $"pages.seq"(0))
      .join(meta, "issue")
      .write.save(args(2))
      // .write.option("compression", "gzip").json(args(2))
    spark.stop()
  }
}
