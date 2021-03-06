package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.StringBuilder
import scala.util.Try
import scala.xml.pull._

object TEIPages {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TEIPages Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.binaryFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val fname = new java.io.File(new java.net.URL(in._1).toURI)
        val id = fname.getName.replaceAll(".xml$", "")
        val buf = new StringBuilder
        var buffering = false
        var seq = -1

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, "pb", attr, _) => {
              val rec = if ( buffering ) {
                Some((f"$id%s_$seq%04d", id, seq, buf.toString.trim))
              } else {
                None
              }
              buffering = true
              seq += 1
              buf.clear
              rec
            }
            case EvElemEnd(_, "text") => {
              if ( buffering ) {
                seq += 1
                val text = buf.toString.trim
                buffering = false
                buf.clear
                Some((f"$id%s_$seq%04d", id, seq, text))
              } else {
                None
              }
            }
            case EvText(t) => {
              if ( buffering ) buf ++= t
              None
            }
            case EvEntityRef(n) => {
              if ( buffering ) buf ++= "&" + n + ";"
              None
            }
            case _ => None
          }
        }
      })
      .toDF("id", "book", "seq", "text")
      .write.save(args(1))
    spark.stop()
  }
}
