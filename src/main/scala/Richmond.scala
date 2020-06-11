package vtpassim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ListBuffer, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

case class RDArticle(id: String, date: String, page: String,
  seq: Int, types: Array[String], text: String)

object Richmond {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Richmond Dispatch Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val divs = Seq("text", "div1", "div2", "div3", "div4", "div5")
    val breakers = Seq("lb", "head", "p", "row")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val raw = scala.io.Source.fromURL(in._1).mkString
        val clean = raw.replaceAll("<\\?xml-model[^>]*>\n?",
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        val pass = new XMLEventReader(scala.io.Source.fromString(clean))

        // val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        val divStack = new Stack[String]()
        val buf = new StringBuilder
        var seq = -1
        var date = ""
        var id = ""
        var page = ""
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, elem, attr, _) if divs.contains(elem) => {
              val res = if ( !buf.isEmpty ) {
                seq += 1
                Some(RDArticle(f"foo#$seq%04d", date, page, seq, divStack.toArray, buf.toString))
              } else
                  None
              buf.clear
              divStack.push(Try(attr("type").text).getOrElse("text"))
              res
            }
            case EvElemEnd(_, elem) if divs.contains(elem) => {
              val res = if ( !buf.isEmpty ) {
                seq += 1
                Some(RDArticle(f"foo#$seq%04d", date, page, seq, divStack.toArray, buf.toString))
              } else
                  None
              buf.clear
              divStack.pop
              res
            }
            case EvElemEnd(_, elem) if breakers.contains(elem) && !divStack.isEmpty && !buf.isEmpty => {
              buf ++= "\n"
              None
            }
            case EvElemStart(_, elem, attr, _) if !divStack.isEmpty && !buf.isEmpty => {
              buf ++= " "
              None
            }
            case EvElemStart(_, "date", attr, _) if divStack.isEmpty && date == "" => {
              date = Try(attr("when").text).getOrElse("")
              None
            }
            case EvElemStart(_, "pb", attr, _) => {
              page = Try(attr("n").text).getOrElse("")
              None
            }
            case EvElemEnd(_, "unclear") if !buf.isEmpty => {
              buf ++= "___"
              None
            }
            case EvText(t) => {
              if ( !divStack.isEmpty ) {
                buf ++= (if (buf.isEmpty) t.replaceAll("^\\s+", "") else t)
              }
              None
            }
            case EvEntityRef(n) => {
              if ( !divStack.isEmpty ) {
                buf ++= (n match {
                  case "amp" => "&"
                  case "lt" => "<"
                  case "gt" => ">"
                  case "apos" => "'"
                  case "quot" => "\""
                  case _ => ""
                })
              }
              None
            }
            case _ => None
          }
        }
      })
      .toDF
      .withColumn("text", regexp_replace(regexp_replace($"text", " [ ]+", " "), "\n[ ]+", "\n"))
      .write.json(args(1))
      spark.stop()
    }
}
