package vtpassim

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map, StringBuilder}

import vtpassim.pageinfo._

case class Surface(id: String, text: String, regions: Array[Region])

// I don't understand the fractional coordinates. Are they
// centimeters? picas? etc.  They're probably percentages, since
// they're all less than 100 but greater than 1.  Then, the lrx and
// lry attributes on the first surface element are abused to give the
// coordinates in thousands of something, probably pixels. Anyway, I'm
// just going to ignore them for now.
object BSB {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NCNP records")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.wholeTextFiles(args(0), sc.defaultParallelism)
      .filter(_._1.contains(".xml"))
      .map { f =>
      val text = new StringBuilder
      val regions = new ArrayBuffer
      val zones = Map[String, Coords]()
      val t = scala.xml.XML.loadString(f._2)
      val ns = t.getNamespace("xml")
      val id = (t \ s"@{$ns}id").text
        (t \\ "line").foreach { line =>
          if ( line.attribute("id").isEmpty ) {
            (line \\ "seg").foreach { seg =>
              if ( seg.text == "" ) {
                text ++= " "
              } else {
                // If I understood the coorindates, I'd do something here.
                val res = seg.text.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
                text ++= res
              }
            }
            if ( (line \ "w" \ "@type").text == "hyphen" ) text ++= "\u00ad"
            text ++= "\n"
          } else {
            // See above: I don't understand the coordinates.
          }
        }
      Surface(id, text.toString, regions.toArray)
    }
      .toDF
      .write.json(args(1))
  }
}
