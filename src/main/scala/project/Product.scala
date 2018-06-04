package project

case class Product(name: String = "", cl: String = "", subCl: String = "",
                   vec: Map[String, Double] = Map()) {
  def +(other: (String, Double)): Product =
    this.copy(vec = this.vec.updated(other._1, other._2))
}

object Product {
  def apply(name: String, seq: Seq[String]): Product = seq match {
    case Seq(a) => apply(name, a, a, Map(a -> 1.0))
    case Seq(a, b, _*) => apply(name, a, b, Map(b -> 1.0))
  }
}
