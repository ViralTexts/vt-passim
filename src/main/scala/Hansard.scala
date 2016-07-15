package vtpassim

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import collection.JavaConversions._
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer

import edu.stanford.nlp.simple.{Document, Sentence}

import vtpassim.pageinfo._

case class ParsedSentence(id: String, seq: Int, house: String, date: String, text: String,
  words: Seq[String], tags: Seq[String], tree: String)

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
                val stree = if ( words.size <= 10 ) s.parse.toString else "" // Do this first to force parser-derived tags
                ParsedSentence(id, seq, house, date, s.toString,
                  words, Seq(""), stree)
              }
            })
          })
      })
      .toDF
      .write.save(args(1))
  }
}
