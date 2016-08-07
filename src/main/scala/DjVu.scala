package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import vtpassim.pageinfo._

object DjVu {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DjVu Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<OBJECT>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</OBJECT>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    val pages = sc.newAPIHadoopFile(args(0),
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])
      .flatMap { x => {
        try {
          val res = new StringBuilder
          val regions = new ArrayBuffer[Region]

          val t = scala.xml.XML.loadString(x._2.toString)
          val bookFile = (t \ "@data").text
          val bookParts = bookFile.split("/+")
          val bookId = bookParts(bookParts.size - 2)

          val pageFile = (t \ "PARAM").filter(x => (x \ "@name").text == "PAGE").map(x => (x \ "@value").text).head
          val pageId = pageFile.replaceAll("\\.djvu$", "").split("_").last
          val seq = pageId.toInt

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
                  res.append(word.text.replaceAll("&", "&amp;").replaceAll("<", "&lt;"))
                  val c = (word \ "@coords").text.split(",").map(_.toInt)
                  regions += Region(start, res.size - start,
                    Coords(c(0), c(3), c(2) - c(0), c(1) - c(3),
                      if ( c.size == 5 ) c(4) - c(3) else c(1) - c(3)))
                }
              res.append("\n")
            }
            res.append("\n")
          }
          Some(Page(bookId + "_" + pageId, bookId, seq, dpi, res.toString, regions.toArray))
        } catch {
          case e: Exception => {
            Console.err.println(x._1 + ": " + e.toString)
            None
          }
        }
      }}
      .toDF
      .coalesce(sc.getConf.getInt("spark.sql.shuffle.partitions", 200))
      .write.save(args(1))
  }
}

