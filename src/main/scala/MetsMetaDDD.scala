package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, regexp_extract, udf}
import collection.JavaConversions._
import scala.util.Try
import vtpassim.pageinfo._

case class DDDMeta(series: String, title: String, lang: Array[String],
  placeOfPublication: String, publisher: String)

object MetsMetaDDD {
  val parseMeta = udf { (s: String) =>
    try {
      val t = scala.xml.XML.loadString(s)
      val meta = (t \\ "dcx").head
      DDDMeta(("DDD:PPN:" + (meta \ "identifier").head.text),
        (meta \ "title").head.text,
        (meta \ "language").map(_.text).toArray,
        (meta \ "spatial").tail.text,
        (meta \ "publisher").head.text
      )
    } catch {
      case e: Exception => {
        println(e.toString)
        DDDMeta(null, null, Array[String](), null, null)
      }
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("MetsMeta import").getOrCreate()
    import spark.implicits._

    spark.read.json(args(0))
      .withColumn("meta_info", parseMeta('xml))
      .select($"meta_info.*")
      .filter('series.isNotNull)
      .dropDuplicates("series")
      .write.json(args(1))
    spark.stop()
  }
}
