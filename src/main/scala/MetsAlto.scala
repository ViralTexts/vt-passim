package vtpassim

import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import vtpassim.pageinfo._

object MetsAlto {
  def altoText(t: scala.xml.NodeSeq) = {
    val buf = new StringBuilder
    val regions = new ArrayBuffer[Region]
      (t \\ "TextLine").foreach { line =>
        (line \ "_").foreach { e =>
          if ( e.label == "String" ) {
            val start = buf.size
            buf.append((e \ "@CONTENT").text)
            try {
              regions += Region(start, buf.size - start,
                Coords((e \ "@HPOS").text.toInt, (e \ "@VPOS").text.toInt,
                  (e \ "@WIDTH").text.toInt, (e \ "@HEIGHT").text.toInt, (e \ "@HEIGHT").text.toInt))
            } catch {
              case e: Exception =>
            }
          } else if ( e.label == "SP" ) {
            buf.append(" ")
          } else if ( e.label == "HYP" ) {
            buf.append("\u00ad")
          }
        }
        buf.append("\n")
      }
    (buf.toString, regions.toArray)
  }
}
