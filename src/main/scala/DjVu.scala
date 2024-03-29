package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.Text

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import vtpassim.pageinfo._

case class DjVuPage(id: String, book: String, seq: Int, text: String, pages: Array[Page])

object DjVu {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DjVu Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val seqpat = ".*_0*([0-9]+)\\.djvu$".r

    val pages = spark.sparkContext.newAPIHadoopFile(args(0),
      classOf[DjVuInputFormat],
      classOf[DjVuEntry],
      classOf[Text])
      .flatMap { x => {
        try {
          val res = new StringBuilder
          val regions = new ArrayBuffer[Region]

          val t = scala.xml.XML.loadString(x._2.toString)
          val bookFile = (t \ "@data").text
          val bookId = x._1.getID

          val width = (t \ "@width").text.toInt
          val height = (t \ "@height").text.toInt

          val pageFile = (t \ "PARAM").filter(x => (x \ "@name").text == "PAGE").map(x => (x \ "@value").text).head
          val pageId = x._1.toString
          val seqpat(strseq) = pageFile
          val seq = strseq.toInt

          val dpi = (t \ "PARAM").filter(x => (x \ "@name").text == "DPI").map(x => (x \ "@value").text).head.toInt

          (t \\ "PARAGRAPH").foreach { para =>
            (para \\ "LINE").foreach { line =>
              var first = true
                (line \ "WORD").foreach { word =>
                  if ( first ) {
                    first = false
                  } else {
                    res.append(" ")
                  }
                  val start = res.size
                  res.append(word.text)
                  val c = (word \ "@coords").text.split(",").map(_.toInt)
                  regions += Region(start, res.size - start,
                    Coords(c(0), c(3), c(2) - c(0), c(1) - c(3),
                      if ( c.size == 5 ) c(4) - c(3) else c(1) - c(3)))
                }
              res.append("\n")
            }
            res.append("\n")
          }
          Some(DjVuPage(pageId, bookId, seq, res.toString,
            Array(Page(pageFile.replace(".djvu", ""), seq, width, height, dpi, regions.toArray))))
        } catch {
          case e: Exception => {
            Console.err.println(x._1 + ": " + e.toString)
            None
          }
        }
      }}
      .toDF
      .coalesce(spark.sparkContext.getConf.getInt("spark.sql.shuffle.partitions", 200))
      .write.mode("overwrite").save(args(1))
    spark.stop()
  }
}

