package vtpassim

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import collection.JavaConversions._

import edu.stanford.nlp.simple.{Document, Sentence}

case class ParsedSentence(words: Seq[String], tags: Seq[String], lemmata: Seq[String],
  tree: String, heads: Seq[Int], labels: Seq[String])


object ParseSentences {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Sentence parsing").getOrCreate()
    import spark.implicits._

    val textCol = "speechtext"

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val splitSentences = udf { (text: String) =>
      if ( text == null ) {
        Seq()
      } else {
        val doc = new Document(text)
        doc.sentences.map { _.text }
      }
    }

    val parseText = udf { (text: String) =>
      try {
        val s = new Sentence(text)
        val words = s.words.toSeq
        if ( words.size <= 50 ) {
          val t = s.parse
          val tags = t.taggedLabeledYield.map(_.toString.replaceFirst("-[0-9]+$", "")).toSeq
          val morph = new edu.stanford.nlp.process.Morphology
          ParsedSentence(words, tags,
            (words.zip(tags).map { case (word, tag) => morph.lemma(word, tag, true) }),
            t.toString,
            s.governors.map(_.orElse(-2).toInt),
            s.incomingDependencyLabels.map(_.orElse("")))
        } else {
          ParsedSentence(words, Seq[String](), Seq[String](),
            "", Seq[Int](), Seq[String]())
        }
      } catch {
        case e: Exception =>
          ParsedSentence(Seq[String](), Seq[String](), Seq[String](),
            "", Seq[Int](), Seq[String]())
        case e: java.lang.Error =>
          ParsedSentence(Seq[String](), Seq[String](), Seq[String](),
            "", Seq[Int](), Seq[String]())
      }
    }

    spark.read.load(args(0))
      .repartition(4000)
      .select(expr("*"), posexplode(splitSentences(col(textCol))) as Seq("seq", "text"))
      .drop(textCol)
      .withColumn("parsed", parseText(col("text")))
      .select(expr("*"), $"parsed.*") // flatten parse fields
      .drop("parsed")
      .write.json(args(1))
    spark.stop()
  }
}
