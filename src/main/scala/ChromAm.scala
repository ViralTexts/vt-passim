package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.Text

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

import vtpassim.pageinfo._

case class CARecord(id: String, issue: String, series: String, ed: String, seq: Int,
  batch: String, date: String, text: String, pages: Array[Page])

object ChronAm {
  def cleanInt(s: String): Int = s.replaceFirst("\\.0*$", "").toInt
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ChronAm Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.newAPIHadoopFile(args(0), classOf[TarballInputFormat],
      classOf[TarballEntry], classOf[Text])
      .filter { _._1.getEntry.endsWith(".xml") }
    // .repartition(sc.getConf.getInt("spark.sql.shuffle.partitions", 200))
      .flatMap { raw =>
      val fname = raw._1.getEntry
      try {
        val contents = raw._2.toString
        val clean = if ( contents.startsWith("\ufeff") ) contents.substring(1) else contents
        val t = scala.xml.XML.loadString(clean)
        val Array(sn, year, month, day, ed, seq, _*) = fname.split("/")
        val series = s"/lccn/$sn"
        val date = s"$year-$month-$day"
        val issue = Seq(series, date, ed) mkString "/"
        val id = s"$issue/$seq"
        val batch = raw._1.getTarball.replaceAll("\\.tar\\.bz2$", "").replaceAll("^.*batch_", "")

        val sb = new StringBuilder
        val regions = new ArrayBuffer[Region]

        val width = Try((t \ "Layout" \ "Page" \ "@WIDTH").text.toInt).getOrElse(0)
        val height = Try((t \ "Layout" \ "Page" \ "@HEIGHT").text.toInt).getOrElse(0)

        val dpi = Try(1200 / "xdpi:([0-9]+)".r.findFirstMatchIn((t \ "Description" \\ "processingStepSettings").text).get.group(1).toInt).getOrElse(0)

          (t \\ "TextBlock") foreach { block =>
            (block \ "TextLine" ) foreach { line =>
              (line \ "_") foreach { e =>
                e.label match {
                  case "String" =>
                    val start = sb.size
                    sb ++= (e \ "@CONTENT").text.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
                    try {
                      regions += Region(start, sb.size - start,
                        Coords(cleanInt(e \\ "@HPOS" text), cleanInt(e \\ "@VPOS" text),
                          cleanInt(e \\ "@WIDTH" text), cleanInt(e \\ "@HEIGHT" text),
                          cleanInt(e \\ "@HEIGHT" text)))
                    } catch {
                      case ex: Exception =>
                    }
                  case "SP" => sb ++= " "
                  case "HYP" => sb ++= "\u00ad"
                  case _ =>
                }
              }
              sb ++= "\n"
            }
            sb ++= "\n"
          }

        val nseq = Try(seq.replace("seq-", "").toInt).getOrElse(0)

        Some(CARecord(id, issue, series, ed.replace("ed-", ""), nseq,
          batch, date, sb.toString, Array(Page(id, nseq, width, height, dpi, regions.toArray))))
      } catch {
        case ex: Exception =>
          Console.err.println("## " + fname + ": " + ex.toString)
          None
      }
    }
      .toDF
      .write.save(args(1))
    spark.stop()
  }
}

