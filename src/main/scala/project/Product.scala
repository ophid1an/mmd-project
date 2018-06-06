package project

case class Product[A](vec: Map[A, Double] = Map()) {
  lazy val normL2: Double = Math.sqrt(vec.values.map(x => x * x).sum)

  def +(other: Map[A, Double]): Product[A] = {
    val intersection = vec.keys.toSet.intersect(other.keys.toSet)
    if (intersection.isEmpty)
      this.copy(vec ++ other)
    else
      sys.error("Product.+ with a map with common key.")
  }
}
