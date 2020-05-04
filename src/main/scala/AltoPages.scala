package vtpassim

import org.apache.spark.sql.SparkSession
import collection.JavaConversions._
import scala.util.Try
import vtpassim.pageinfo._

case class AltoPage(id: String, book: String, seq: Int, text: String, pages: Array[Page])

object AltoPages {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("MetsAlto import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(f => f._1.endsWith(".xml") || f._1.endsWith(".alto"))
      .flatMap( in => {
        val t = scala.xml.XML.load(in._1)
          (t \\ "Page").map { page =>
            val id = (page \ "@ID").text
            val seq = (page \ "@PHYSICAL_IMG_NR").text.toInt
            val (text, regions) = MetsAlto.altoText(page)
            AltoPage(id, in._1, seq, text,
              Array(Page(id, seq,
                (page \ "@WIDTH").text.toInt, (page \ "@HEIGHT").text.toInt, 0, regions)))
          }
      })
      .toDF
      .write.save(args(1))
    spark.stop()
  }
}
