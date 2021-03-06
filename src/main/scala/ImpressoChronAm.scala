package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, struct, collect_list, max, sort_array, lit,
  regexp_replace, from_unixtime, unix_timestamp, concat, year}
import org.apache.hadoop.io.Text

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

case class CToken(c: Array[Float], s: Int, l: Int)

case class ImpressoToken(c: Array[Float], tx: String)
case class ImpressoLine(c: Array[Float], t: Array[ImpressoToken])
case class ImpressoParagraph(l: Array[ImpressoLine])
case class ImpressoRegion(c: Array[Float], p: Array[ImpressoParagraph], pOf: String)

case class RawImpresso(id: String, caid: String, issue: String, pid: String, seq: Int,
  series: String, date: String, rb: Array[Int], lb: Array[Int], text: String,
  ctokens: Array[CToken], r: Array[ImpressoRegion])

object ImpressoChronAm {
  def roundPct(f: Float): Float = (Math.round(100000.0 * f).toFloat / 1000)
  def getCoords(e: scala.xml.Node, w: Int, h: Int): Array[Float] = Try(Array[Float](roundPct((e \ "@HPOS" text).toFloat / w),
    roundPct((e \ "@VPOS" text).toFloat / h),
    roundPct((e \ "@WIDTH" text).toFloat / w),
    roundPct((e \ "@HEIGHT" text).toFloat / h))).getOrElse(Array[Float](0,0,0,0))
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
          val series = sn
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

          val w = Try((t \ "Layout" \ "Page" \ "@WIDTH").text.toInt).getOrElse(1)
          val h = Try((t \ "Layout" \ "Page" \ "@HEIGHT").text.toInt).getOrElse(1)

          val coords = getCoords(_: scala.xml.Node, w, h)

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
                    val c = coords(e)
                    ctokens += CToken(c, start, sb.size - start)
                    tokens += ImpressoToken(c, tx)
                  case "SP" => sb ++= " "
                  case "HYP" => sb ++= "-" // "\u00ad"
                  case _ =>
                }
              }
              sb ++= " " //"\n"
              lines += ImpressoLine(coords(line), tokens.toArray)
            }
            sb ++= "" //"\n"
            regions += ImpressoRegion(coords(block),
              Array(ImpressoParagraph(lines.toArray)), pid)
          }

          Some(RawImpresso(id, caid, issue, pid, seq, series, date,
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
        .withColumn("year", year('date))
        .repartition('series, 'year)

    records.groupBy("issue", "series", "year")
      .agg(max('cdt) as "cdt",
        sort_array(collect_list(struct(struct('id, array("seq") as "pp",
          lit("page") as "tp") as "m"))) as "i",
        sort_array(collect_list("pid")) as "pp")
      .select('series, 'year, 'issue as "id", 'cdt, 'i, 'pp, lit("open_public") as "ar")
      .write.partitionBy("series", "year")
      .option("compression", "bzip2")
      .mode("ignore")
      .json(args(2) + "/issues")

    records.select('series, 'year, 'pid as "id",
      concat(lit("https://chroniclingamerica.loc.gov/iiif/2/"),
        regexp_replace('file, "/", "%2F")) as "iiif",
      lit(true) as "cc", 'cdt, 'r)
      .write.partitionBy("series", "year")
      .option("compression", "bzip2")
      .mode("ignore")
      .json(args(2) + "/pages")

    records.select('series, 'year, 'id, lit("ar") as "tp", lit(true) as "cc", lit(false) as "olr",
      array('seq) as "pp",
      'date as "d",
      concat(regexp_replace('cdt, " ", "T"), lit("Z")) as "ts",
      'text as "ft", 'lb, 'rb as "pb", 'rb,
      array(struct('pid as "id", 'seq as "n", 'ctokens as "t")) as "ppreb")
      .write.partitionBy("series", "year")
      .option("compression", "bzip2")
      .mode("ignore")
      .json(args(2) + "/contentitems")

    spark.stop()
  }
}

