package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import edu.stanford.nlp.simple.{Document, Sentence}

case class ParsedSentence(id: String, seq: Int, house: String, date: String, text: String,
  words: Seq[String], tags: Seq[String], tree: String, heads: Seq[Int], labels: Seq[String])

object HansardSentences {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hansard parsing")
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
                val words: Seq[String] = s.words
                if ( words.size <= 50 ) {
                  val t = s.parse
                  ParsedSentence(id, seq, house, date, s.toString, words,
                    t.taggedLabeledYield.map(_.toString.replaceFirst("-[0-9]+$", "")),
                    t.toString,
                    s.governors.map(_.get.toInt),
                    s.incomingDependencyLabels.map(_.get))
                } else {
                  ParsedSentence(id, seq, house, date, s.toString, words,
                    Seq[String](), "", Seq[Int](), Seq[String]())
                }
              }
            })
          })
      })
      .toDF
      .write.save(args(1))
  }
}
