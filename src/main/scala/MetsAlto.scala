package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

import vtpassim.pageinfo._

case class Record(id: String, issue: String, series: String, seq: Int,
  date: String, lang: String, dpi: Int, text: String, regions: Array[Region])

object MetsAlto {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MetsAlto Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.binaryFiles(args(0), sc.defaultParallelism)
      .filter(_._1.endsWith(".zip"))
      .flatMap( x => {
        try {
          val fname = new java.io.File(new java.net.URL(x._1).toURI)
          val issue = fname.getName.replaceAll(".zip$", "")

          val zfile = new java.util.zip.ZipFile(fname)
          val mfile = zfile.entries.filter(_.getName.endsWith("mets.xml")).toSeq.head
          val t = scala.xml.XML.load(zfile.getInputStream(mfile))
          val series = (t \\ "identifier").head.text
          val lang = (t \\ "languageTerm").head.text
          val dpi = Try((t \\ "XphysScanResolution").head.text.toInt).getOrElse(0)

          val date = (t \\ "dateIssued").take(1).text
            .replaceAll("""^(\d\d)\.(\d\d)\.(\d\d\d\d)$""", """$3-$2-$1""")
            .replaceAll("""^(\d\d)\.(\d\d\d\d)$""", """$2-$1""")

          zfile.entries
            .filter(f => f.getName.endsWith(".xml") && !f.getName.endsWith("mets.xml"))
            .map { f =>
            val t = scala.xml.XML.load(zfile.getInputStream(f))
            val buf = new StringBuilder
            val regions = new ArrayBuffer[Region]
            val seq = Try((t \\ "Page" \ "@PHYSICAL_IMG_NR").text.toInt).getOrElse(0)
              (t \\ "TextLine").foreach { line =>
                (line \ "_").foreach { e =>
                  if ( e.label == "String" ) {
                    val start = buf.size
                    buf.append((e \ "@CONTENT").text.replaceAll("&", "&amp;").replaceAll("<", "&lt;"))
                    try {
                      regions += Region(start, buf.size - start,
                        Coords((e \ "@HPOS").text.toInt, (e \ "@VPOS").text.toInt,
                          (e \ "@WIDTH").text.toInt, (e \ "@HEIGHT").text.toInt, (e \ "@HEIGHT").text.toInt))
                    } catch {
                      case e: Exception =>
                    }
                  } else if ( e.label == "SP" ) {
                    buf.append(" ")
                  } else if ( e.label == "HYP" ) {
                    buf.append("\u00ad")
                  }
                }
                buf.append("\n")
              }
            Record(issue + "/" + f.getName.replaceAll(".xml$", ""), issue, series, seq,
              date, lang, dpi, buf.toString, regions.toArray)
          }
        } catch {
          case e: Exception => {
            println(x._1 + e.toString)
            None
          }
        }
      }
    )
      .toDF
      .write.save(args(1))
  }
}
