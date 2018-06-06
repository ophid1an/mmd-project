package project

case class Product[A](vec: Map[A, Double] = Map()) {
  def +(other: Map[A, Double]): Product[A] = {
    val intersection = vec.keys.toSet.intersect(other.keys.toSet)
    if (intersection.isEmpty)
      this.copy(vec ++ other)
    else
      sys.error("Product.+ with a map with common key.")
  }
}
