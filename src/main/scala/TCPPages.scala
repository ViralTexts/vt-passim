package vtpassim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.StringBuilder
import scala.xml.pull._

object TCPPages {
  def pageAccess(id: String): String = {
    val p = id.split(":")
    if ( p.size == 3 && p(0) == "tcp" )
      s"http://eebo.chadwyck.com.ezproxy.neu.edu/search/full_rec?SOURCE=pgimages.cfg&ACTION=ByID&ID=V${p(1)}&PAGENO=${p(2)}"
    else
      ""
  }
  def pageImage(id: String): String = {
    val p = id.split(":")
    if ( p.size == 3 && p(0) == "tcp" )
      s"http://eebo.chadwyck.com.ezproxy.neu.edu/fetchimage?vid=${p(1)}&page=${p(2)}"
    else
      ""
  }
  def pageThumb(id: String): String = {
    val p = id.split(":")
    if ( p.size == 3 && p(0) == "tcp" )
      s"http://eebo.chadwyck.com.ezproxy.neu.edu/fetchimage?vid=${p(1)}&page=${p(2)}&width=80"
    else
      ""
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TCPPages Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val bookPrefix = "http://name.umdl.umich.edu/"

    sc.binaryFiles(args(0), sc.defaultParallelism)
      .filter(_._1.endsWith(".xml"))
      .flatMap( in => {
        val buf = new StringBuilder
        var buffering = false
        var seq = 0

        val pass1 = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass1.foreach { event =>
          event match {
            case EvElemStart(_, "pb", attr, _) => seq += 1
            case EvElemStart(_, "teiHeader", attr, _) => {
              buffering = true
              buf ++= "<teiHeader" + attr + ">"
            }
            case EvElemEnd(_, "teiHeader") => {
              buffering = false
              buf ++= "</teiHeader>"
            }
            case EvElemStart(_, tag, attr, _) => if ( buffering ) buf ++= "<" + tag + attr + ">"
            case EvElemEnd(_, tag) => if ( buffering ) buf ++= "</" + tag + ">"
            case EvText(t) => if ( buffering ) buf ++= t
            case EvEntityRef(n) => if ( buffering ) buf ++= "&" + n + ";"
            case _ => None
          }
        }
        val t = scala.xml.XML.loadString(buf.toString)
        val rawid = (t \\ "idno").filter(x => (x \ "@type").text == "DLPS")
          .map(_.text).mkString("; ")
        val id = (if ( rawid == "" ) in._1 else rawid)
        val title = (t \\ "fileDesc" \ "titleStmt" \ "title").map(_.text).mkString("; ")
        val creator = (t \\ "fileDesc" \ "titleStmt" \ "author").map(_.text).mkString("; ")
        val date = (t \\ "editionStmt" \ "edition" \ "date").map(_.text).mkString("; ")
        val pagecount = seq

        buf.clear
        buffering = false
        seq = -1
        var img = ""

        val pass2 = new XMLEventReader(scala.io.Source.fromURL(in._1))
        pass2.flatMap { event =>
          event match {
            case EvElemStart(_, "pb", attr, _) => {
              val rec = if ( buffering ) {
                Some((id + "_" + seq, id, seq, title, creator, date,
                  bookPrefix+id, pagecount, pageAccess(img), pageImage(img), pageThumb(img),
                  buf.toString.trim))
              } else {
                None
              }
              buffering = true
              seq += 1
              buf.clear
              img = attr.get("facs").map(_.text).headOption.getOrElse("")
              rec
            }
            case EvElemEnd(_, "text") => {
              if ( buffering ) {
                seq += 1
                val text = buf.toString.trim
                buf.clear
                Some((id + "_" + seq, id, seq, title, creator, date,
                  bookPrefix+id, pagecount, pageAccess(img), pageImage(img), pageThumb(img),
                  text))
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
      .toDF("id", "series", "seq", "title", "creator", "date",
        "book_access", "pagecount", "page_access", "page_image", "page_thumb", "text")
      .write.save(args(1))
  }
}
