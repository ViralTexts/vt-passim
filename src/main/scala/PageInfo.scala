package vtpassim.pageinfo

case class Coords(x: Int, y: Int, w: Int, h: Int, b: Int)

case class Region(start: Int, length: Int, coords: Coords)

case class Page(id: String, book: String, seq: Int, dpi: Int, text: String,
  width: Int, height: Int, regions: Array[Region])
