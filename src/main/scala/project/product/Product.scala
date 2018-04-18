package project.product

case class Product(name: String = "", cl: String = "", subCl: String = "")

object Product {
  def apply(name: String, seq: Seq[String]): Product = seq match {
    case Seq(a) => apply(name, a, a)
    case Seq(a, b, _*) => apply(name, a, b)
  }
}
