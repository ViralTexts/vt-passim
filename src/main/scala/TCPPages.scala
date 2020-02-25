package vtpassim

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.StringBuilder
import scala.util.Try
import scala.xml.pull._

object TCPPages {
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
        var pageid = ""

        val pass = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass.flatMap { event =>
          event match {
            case EvElemStart(_, "pb", attr, _) => {
              val rec = if ( buffering ) {
                Some((f"$id%s_$seq%04d", id, pageid, seq, buf.toString.trim))
              } else {
                None
              }
              buffering = true
              seq += 1
              pageid = attr.get("facs").map(_.text).headOption.getOrElse("")
              buf.clear
              rec
            }
            case EvElemEnd(_, "TEI") => {
              if ( buffering ) {
                val text = buf.toString.trim
                buffering = false
                buf.clear
                Some((f"$id%s_$seq%04d", id, pageid, seq, text))
              } else {
                None
              }
            }
            case EvElemStart(_, "g", attr, _) => {
              if ( buffering
                && (attr.get("ref").map(_.text).headOption.getOrElse("") == "char:EOLhyphen") ) {
                buf ++= "\u00ad\n"
              }
              None
            }
            // HACK: we assume <gap> is after the first pb and isn't recursive
            // Ought to use a stack, of course.
            case EvElemStart(_, "gap", attr, _) => { buffering = false; None }
            case EvElemStart(_, "desc", attr, _) => { buffering = true; None }
            case EvElemEnd(_, "desc") => { buffering = false; None }
            case EvElemEnd(_, "gap") => { buffering = true; None }
            case EvText(t) => {
              if ( buffering ) buf ++= t
              None
            }
            case EvEntityRef(n) => {
              if ( buffering ) buf ++= (n match {
                case "amp" => "&"
                case "lt" => "<"
                case "gt" => ">"
                case "apos" => "'"
                case "quot" => "\""
                case _ => ""
                })
              None
            }
            case _ => None
          }
        }
      })
      .toDF("id", "book", "pageid", "seq", "text")
      .write.save(args(1))
    spark.stop()
  }
}

// Saved URL construction code
  // def pageAccess(id: String): String = {
  //   val p = id.split(":")
  //   if ( p.size == 3 && p(0) == "tcp" )
  //     s"http://eebo.chadwyck.com.ezproxy.neu.edu/search/full_rec?SOURCE=pgimages.cfg&ACTION=ByID&ID=V${p(1)}&PAGENO=${p(2)}"
  //   else
  //     ""
  // }
  // def pageImage(id: String): String = {
  //   val p = id.split(":")
  //   if ( p.size == 3 && p(0) == "tcp" )
  //     s"http://eebo.chadwyck.com.ezproxy.neu.edu/fetchimage?vid=${p(1)}&page=${p(2)}"
  //   else
  //     ""
  // }
  // def pageThumb(id: String): String = {
  //   val p = id.split(":")
  //   if ( p.size == 3 && p(0) == "tcp" )
  //     s"http://eebo.chadwyck.com.ezproxy.neu.edu/fetchimage?vid=${p(1)}&page=${p(2)}&width=80"
  //   else
  //     ""
  // }
  // val bookPrefix = "http://name.umdl.umich.edu/"
