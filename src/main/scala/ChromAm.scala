package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

import vtpassim.pageinfo._

case class CARecord(id: String, issue: String, series: String, ed: String, seq: Int,
  date: String, dpi: Int, page_access: String, text: String, regions: Array[Region])

object ChronAm {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ChronAm Import")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val upref = "http://chroniclingamerica.loc.gov"

    sc.binaryFiles(args(0), sc.defaultParallelism)
      .filter(_._1.endsWith(".tar.bz2"))
      .flatMap { case (tarfile, binstream) =>
      val tarin = new TarArchiveInputStream(new BZip2CompressorInputStream(new java.io.FileInputStream(new java.io.File(new java.net.URL(tarfile).toURI))))
        (1 to 1000000).toStream.map { idx => // make it lazy
          val e = tarin.getNextTarEntry
          if ( e == null ) {
            (tarfile, "", "", idx)
          } else if ( e.getName.endsWith(".xml") ) {
            val bin = new java.io.BufferedReader(new java.io.InputStreamReader(tarin))
            val sb = new StringBuilder
            var line = ""
            while ( { line = bin.readLine; line != null } ) {
              sb append line
              sb append "\n"
            }
            (tarfile, e.getName, sb.toString, idx)
          } else {
            (tarfile, e.getName, "", idx)
          }
        }
        .takeWhile(_._2 != "")
        .filter(_._3 != "")
    }
      .repartition(sc.getConf.getInt("spark.sql.shuffle.partitions", 200))
    // .toDF("fname", "contents", "idx")
      .flatMap { case (tarfile, fname, contents, idx) =>
        println(tarfile + ": " + fname)
        try {
          val clean = if ( contents.startsWith("\ufeff") ) contents.substring(1) else contents
          val t = scala.xml.XML.loadString(clean)
          val Array(sn, year, month, day, ed, seq, _*) = fname.split("/")
          val series = s"/lccn/$sn"
          val date = s"$year-$month-$day"
          val issue = Seq(series, date, ed) mkString "/"
          val id = s"$issue/$seq"

          val sb = new StringBuilder
          val regions = new ArrayBuffer[Region]

          (t \\ "TextBlock") foreach { block =>
            (block \ "TextLine" ) foreach { line =>
              (line \ "_") foreach { e =>
                e.label match {
                  case "String" =>
                    val start = sb.size
                    sb ++= (e \ "@CONTENT").text.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
                    try {
                      regions += Region(start, sb.size - start,
                        Coords((e \ "@HPOS").text.toInt, (e \ "@VPOS").text.toInt,
                          (e \ "@WIDTH").text.toInt, (e \ "@HEIGHT").text.toInt,
                          (e \ "@HEIGHT").text.toInt))
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

          Some(CARecord(id, issue, series, ed.replace("ed-", ""),
            Try(seq.replace("seq-", "").toInt).getOrElse(0),
            date, 0, upref + id, sb.toString, regions.toArray))
        } catch {
          case ex: Exception =>
            println("## " + tarfile + ": " + fname + ": " + ex.toString)
            None
        }
    }
      .toDF
      .write.save(args(1))
  }
}

