package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Stack, StringBuilder}
import scala.util.Try
import scala.xml.pull._

case class RDArticle(id: String, seq: Int, types: Array[String], text: String)

object Richmond {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Richmond Dispatch Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val divs = Seq("div1", "div2", "div3", "div4", "div5")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        val divStack = new Stack[String]()
        val buf = new StringBuilder
        var seq = -1
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, "text", attr, _) => {
              seq += 1
              divStack.push("text")
              Nil
            }
            case EvElemEnd(_, "text") => {
              val res = Some(RDArticle("foo", seq, divStack.toList, buf.toString))
              divStack.pop
              buf.clear
              res
            }
            case EvElemStart(_, elem, attr, _) if divs.contains(elem) => {
              Nil
            }
            case EvText(t) => {
              if ( !divStack.isEmpty ) buf ++= (if (buf.isEmpty) t.replaceAll("^\\\s+", "") else t)
              Nil
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
              Nil
            }
            case _ => Nil
          }
        }
      })
      .toDF
      .write.json(args(1))
      spark.stop()
    }
}
