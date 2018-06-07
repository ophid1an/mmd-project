package project

case class Product[A](vec: Map[A, Double] = Map()) {
  def +(other: Map[A, Double]): Product[A] = {
    val intersection = vec.keys.toSet.intersect(other.keys.toSet)
    if (intersection.isEmpty)
      this.copy(vec ++ other)
    else
      sys.error("Product.+ operates on a Map with no common keys with the Product object.")
  }
}
