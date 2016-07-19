package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import edu.stanford.nlp.simple.{Document, Sentence}

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
                (id, seq, house, date, s.text)
              }
            })
          })
      })
      .toDF("id", "seq", "house", "date", "text")
      .write.save(args(1))
  }
}
