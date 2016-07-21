package vtpassim.pageinfo

case class Coords(x: Int, y: Int, w: Int, h: Int, b: Int)

case class Region(start: Int, length: Int, coords: Coords)

case class Page(id: String, series: String, seq: Int, dpi: Int, text: String,
  regions: Array[Region])
