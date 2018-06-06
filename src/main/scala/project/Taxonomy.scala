package project

case class Taxonomy(products: Map[String, Int] = Map(), classes: Map[String, Int] = Map(),
                    subClasses: Map[String, Int] = Map(),
                    productsToSubClasses: Map[Int, Int] = Map(),
                    subClassesToClasses: Map[Int, Int] = Map()
                   ) {
  lazy val subClassesToProducts: Map[Int, List[Int]] = invertMap(productsToSubClasses)
  lazy val classesToSubClasses: Map[Int, List[Int]] = invertMap(subClassesToClasses)

  lazy val idsToSubClasses: Map[Int, String] = invertMap(subClasses).map { case (k, v) => k -> v.head }
  lazy val idsToClasses: Map[Int, String] = invertMap(classes).map { case (k, v) => k -> v.head }
  lazy val idsToProducts: Map[Int, String] = invertMap(products).map { case (k, v) => k -> v.head }

  def ++(other: Taxonomy): Taxonomy = {
    if (other.products.size == 1 && other.classes.size == 1 && other.subClasses.size == 1
      && other.subClassesToClasses.size == 1 && other.productsToSubClasses.size == 1) {
      val newProducts = other.products.mapValues(_ => products.size) ++ products
      val newClasses = other.classes.mapValues(_ => classes.size) ++ classes
      val newSubClasses = other.subClasses.mapValues(_ => subClasses.size) ++ subClasses
      val otherProductCorrectId = newProducts.getOrElse(other.products.keysIterator.next(), -1)
      val otherClassCorrectId = newClasses.getOrElse(other.classes.keysIterator.next(), -1)
      val otherSubClassCorrectId = newSubClasses.getOrElse(other.subClasses.keysIterator.next(), -1)

      this.copy(products = newProducts, classes = newClasses, subClasses = newSubClasses,
        productsToSubClasses = productsToSubClasses.updated(otherProductCorrectId, otherSubClassCorrectId),
        subClassesToClasses = subClassesToClasses.updated(otherSubClassCorrectId, otherClassCorrectId)
      )
    }
    else
      sys.error("Taxonomy.++ operates on Taxonomy object with one product.")
  }

  private def invertMap[A, B](inputMap: Map[A, B]): Map[B, List[A]] = {
    inputMap.foldLeft(Map[B, List[A]]()) {
      case (mapAccumulator, (value, key)) =>
        if (mapAccumulator.contains(key)) {
          mapAccumulator.updated(key, mapAccumulator(key) :+ value)
        } else {
          mapAccumulator.updated(key, List(value))
        }
    }
  }
}

object Taxonomy {
  def apply(prodName: String, seq: Seq[String]): Taxonomy = seq match {
    case Seq(a) => apply(Map(prodName -> 0), Map(a -> 0), Map(a -> 0), Map(0 -> 0), Map(0 -> 0))
    case Seq(a, b, _*) => apply(Map(prodName -> 0), Map(a -> 0), Map(b -> 0), Map(0 -> 0), Map(0 -> 0))
  }
}