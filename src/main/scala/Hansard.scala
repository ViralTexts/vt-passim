package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.xml.pull._

import edu.stanford.nlp.simple.{Document, Sentence}

object HansardPages {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hansard pages")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.wholeTextFiles(args(0), sc.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap( f => {
        val book = (new java.io.File(f._1)).getName.replaceFirst("""\.xml.*$""", "")
        val buf = new StringBuilder
        var seq = -1
        var pid = ""
        var date = ""

        (new XMLEventReader(scala.io.Source.fromString(f._2))).flatMap {
          case EvElemStart(_, "image", attr, _) =>
            val page = if ( pid != "" ) {
              Some((pid, book, seq,
                "Hansard's Parliamentary Debates", date, buf.toString.trim))
            } else {
              None
            }
            seq += 1
            pid = attr.get("src").map(_.text).headOption.getOrElse("")
            buf.clear
            page
          case EvElemEnd(_, "hansard") =>
            if ( pid != "" ) {
              Some((pid, book, seq,
                "Hansard's Parliamentary Debates", date, buf.toString.trim))
            } else {
              None
            }
          case EvElemStart(_, "date", attr, _) =>
            date = attr.get("format").map(_.text).headOption.getOrElse("")
            None
          case EvElemStart(_, "p", _, _) => buf ++= "<p>"; None
          case EvElemEnd(_, "p") => buf ++= "</p>"; None
          case EvElemStart(_, "title", _, _) => buf ++= "<h4>"; None
          case EvElemEnd(_, "title") => buf ++= "</h4>"; None
          case EvText(t) => buf ++= t; None
          case EvEntityRef(n) => buf ++= "&" + n + ";"; None
          case _ => None
        }
      })
      .toDF("id", "book", "seq", "title", "date", "text")
      .write.save(args(1))
  }
}

object HansardSentences {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hansard sentences")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.wholeTextFiles(args(0), sc.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .flatMap( f => {
        (scala.xml.XML.loadString(f._2) \ "_")
          .filter(n => (n.label == "houselords") || (n.label == "housecommons"))
          .flatMap( session => {
            val house = session.label
            val date = (session \ "date" \ "@format").text
            (session \\ "p").flatMap( p => {
              val id = (p \ "@id").text
              val doc = new Document(p.text)
              doc.sentences.zipWithIndex.map { case (s, seq) =>
                (id, seq, house, date, s.text)
              }
            })
          })
      })
      .toDF("id", "seq", "house", "date", "text")
      .write.save(args(1))
  }
}
