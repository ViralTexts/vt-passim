package vtpassim.pageinfo

case class Coords(x: Int, y: Int, w: Int, h: Int, b: Int)

case class Region(start: Int, length: Int, coords: Coords)

case class Page(id: String, seq: Int, width: Int, height: Int, dpi: Int, regions: Array[Region])
