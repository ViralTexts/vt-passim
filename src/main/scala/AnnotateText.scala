package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.functions._

import collection.JavaConversions._

import edu.stanford.nlp.simple.{Document, Sentence}

case class ParsedSentence(words: Seq[String], tags: Seq[String], lemmata: Seq[String],
  tree: String, heads: Seq[Int], labels: Seq[String])


object ParseSentences {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sentence parsing")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val textCol = "text"

    sqlContext.read.load(args(0))
      .repartition(sc.defaultParallelism)
      .explode(col(textCol)) { case Row(text: String) =>
        try {
          val s = new Sentence(text)
          val words = s.words.toSeq
          if ( words.size <= 50 ) {
            val t = s.parse
            val tags = t.taggedLabeledYield.map(_.toString.replaceFirst("-[0-9]+$", "")).toSeq
            val morph = new edu.stanford.nlp.process.Morphology
            Some(ParsedSentence(words, tags,
              (words.zip(tags).map { case (word, tag) => morph.lemma(word, tag, true) }),
              t.toString,
              s.governors.map(_.orElse(-2).toInt),
              s.incomingDependencyLabels.map(_.orElse(""))))
          } else {
            Some(ParsedSentence(words, Seq[String](), Seq[String](),
              "", Seq[Int](), Seq[String]()))
          }
        } catch {
          case e: Exception =>
            Some(ParsedSentence(Seq[String](), Seq[String](), Seq[String](),
              "", Seq[Int](), Seq[String]()))
          case e: java.lang.Error =>
            Some(ParsedSentence(Seq[String](), Seq[String](), Seq[String](),
              "", Seq[Int](), Seq[String]()))
        }
    }
    .write.save(args(1))
  }
}
