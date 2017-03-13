package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import scala.util.Try

import vtpassim.pageinfo._

case class LocDoc(id: String, book: String, text: String, locs: Array[Locus])

object CTSText {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("CTS Text Import").getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    spark.sparkContext.wholeTextFiles(args(0), spark.sparkContext.defaultParallelism)
      .filter(_._1.endsWith(".cts"))
      .map( in => {
        val id = in._1.replaceAll("\\.cts$", "").replaceAll("%3a", ":")
          .replaceAll("^.*urn:", "urn:")
        val group = id.replaceAll("\\.[^.]+$", "")
        val buf = new StringBuilder
        val locs = new ArrayBuffer[Locus]

        in._2.split("\n").foreach { line =>
          val fields = line.split("\t", 2)
          val loc = fields(0)
          val text = fields(1).replaceAll(" [ ]+", " ")
          val start = buf.size
          buf.append(text)
          buf.append("\n")
          locs += Locus(start, buf.size - start, loc)
        }

        LocDoc(id, group, buf.toString, locs.toArray)
      })
      .toDF
      .write.save(args(1))
    spark.stop()
  }
}
