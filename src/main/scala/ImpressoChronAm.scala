package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, struct, collect_list, max, sort_array, lit,
  regexp_replace, from_unixtime, unix_timestamp, concat}
import org.apache.hadoop.io.Text

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

case class CToken(c: Array[Int], s: Int, l: Int)

case class ImpressoToken(c: Array[Int], tx: String)
case class ImpressoLine(c: Array[Int], t: Array[ImpressoToken])
case class ImpressoParagraph(l: Array[ImpressoLine])
case class ImpressoRegion(c: Array[Int], p: Array[ImpressoParagraph], pOf: String)

case class RawImpresso(id: String, caid: String, issue: String, pid: String, seq: Int,
  date: String, rb: Array[Int], lb: Array[Int], text: String,
  ctokens: Array[CToken], r: Array[ImpressoRegion])

object ImpressoChronAm {
  def cleanInt(s: String): Int = s.replaceFirst("\\.0*$", "").toInt
  def getCoords(e: scala.xml.Node): Array[Int] = Try(Array[Int](cleanInt(e \ "@HPOS" text),
    cleanInt(e \ "@VPOS" text),
    cleanInt(e \ "@WIDTH" text),
    cleanInt(e \ "@HEIGHT" text))).getOrElse(Array[Int](0,0,0,0))
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ChronAm Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val edletters = "abcdefghijklmnopqrstuvwxyz"

    val files = spark.read.json(args(1))

    val records =
      spark.sparkContext.newAPIHadoopFile(args(0), classOf[TarballInputFormat],
        classOf[TarballEntry], classOf[Text])
        .filter { _._1.getEntry.endsWith(".xml") }
        .flatMap { raw =>
        val fname = raw._1.getEntry
        try {
          val batch = raw._1.getTarball.replaceAll("\\.tar\\.bz2$", "")
          val contents = raw._2.toString
          val clean = if ( contents.startsWith("\ufeff") ) contents.substring(1) else contents
          val t = scala.xml.XML.loadString(clean)
          val Array(sn, year, month, day, caed, caseq, _*) = fname.split("/")
          val series = s"/lccn/$sn"
          val date = s"$year-$month-$day"
          val caissue = Seq("/ca", batch, sn, date, caed) mkString "/"
          val caid = s"$caissue/$caseq"

          val ed = caed.replace("ed-", "")
          val seq = Try(caseq.replace("seq-", "").toInt).getOrElse(0)

          val issue = s"$series-$date-" + edletters(Try(ed.toInt).getOrElse(1) - 1)
          val id = f"$issue-i$seq%04d"
          val pid = f"$issue-p$seq%04d"

          val sb = new StringBuilder
          val ctokens = new ArrayBuffer[CToken]
          val rb = new ArrayBuffer[Int]
          val lb = new ArrayBuffer[Int]
          val regions = new ArrayBuffer[ImpressoRegion]

          (t \\ "TextBlock") foreach { block =>
            val lines = new ArrayBuffer[ImpressoLine]
            rb += sb.size
            (block \ "TextLine" ) foreach { line =>
              val tokens = new ArrayBuffer[ImpressoToken]
              lb += sb.size
              (line \ "_") foreach { e =>
                e.label match {
                  case "String" =>
                    val start = sb.size
                    val tx = (e \ "@CONTENT").text
                    sb ++= tx
                    val c = getCoords(e)
                    ctokens += CToken(c, start, sb.size - start)
                    tokens += ImpressoToken(c, tx)
                  case "SP" => sb ++= " "
                  case "HYP" => sb ++= "\u00ad"
                  case _ =>
                }
              }
              sb ++= "\n"
              lines += ImpressoLine(getCoords(line), tokens.toArray)
            }
            sb ++= "\n"
            regions += ImpressoRegion(getCoords(block),
              Array(ImpressoParagraph(lines.toArray)), pid)
          }

          Some(RawImpresso(id, caid, issue, pid, seq, date,
            rb.toArray, lb.toArray,
            sb.toString, ctokens.toArray, regions.toArray))
        } catch {
          case ex: Exception =>
            Console.err.println("## " + fname + ": " + ex.toString)
            None
        }
      }
        .toDF
        .dropDuplicates("caid")
        .join(files.select($"id" as "caid", $"file"), "caid")
        .dropDuplicates("id")
        .withColumn("cdt", from_unixtime(unix_timestamp()))

    records.groupBy("issue")
      .agg(max('cdt) as "cdt",
        sort_array(collect_list(struct('id, array("seq") as "pp", lit("page") as "tp") as "m")) as "i",
        sort_array(collect_list("pid")) as "pp")
      .select('issue as "id", 'cdt, 'i, 'pp, lit("open_public") as "ar")
      .write.json(args(2) + "/issues")

    records.select('pid as "id",
      regexp_replace('file, "/", "%2F") as "iiif",
      lit(true) as "cc", 'cdt, 'r)
      .write.json(args(2) + "/pages")

    records.select('id, lit("ar") as "tp", lit(true) as "cc", lit(false) as "olr",
      array('seq) as "pp",
      'date as "d",
      concat(regexp_replace('cdt, " ", "T"), lit("Z")) as "ts",
      'text as "ft", 'lb, 'rb as "pb", 'rb,
      array(struct('pid as "id", 'seq as "n", 'ctokens as "t")) as "ppreb")
      .write.json(args(2) + "/contentitems")

    //records.write.json(args(2))
    spark.stop()
  }
}

