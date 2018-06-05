package project

import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class Spending[A](vec: Map[A, Double]) {
  def cnt: Double = vec.values.sum

  def ++(other: Spending[A]): Spending[A] =
    this.copy(Spending.mergeMaps(vec, other.vec))

  def *(other: Map[A, Double]): Spending[A] = // Is not commutative!!!
    this.copy(multiplyMaps(vec, other))

  def fractional: Spending[A] = this.copy(vec.mapValues(_ / cnt))

  // Get MLlib sparse vector used for clustering
  def sparseVec(size: Int): Vector = {
    val modifiedVec = vec.map {
      case (k, v) =>
        // Limit maximum value to 5 in order to limit
        // the influence of very high product-class spending
        // (page 14 of the paper starting at line 9 from bottom)
        if (v > 5.0) k -> 5.0
        else k -> v
    }
    Vectors.sparse(size, modifiedVec.toSeq.asInstanceOf[Seq[(Int, Double)]])
  }

  def toClString(taxonomy: Taxonomy): Spending[String] =
    this.copy(vec.map {
      case (k, v) => k match {
        case i: Int => taxonomy.idsToClasses.getOrElse(i, "") -> v
        case _ => sys.error("Spending[A] with A != Int.")
      }
    })

  def toSubClString(taxonomy: Taxonomy): Spending[String] =
    this.copy(vec.map {
      case (k, v) => k match {
        case i: Int => taxonomy.idsToSubClasses.getOrElse(i, "") -> v
        case _ => sys.error("Spending[A] with A != Int.")
      }
    })

  private def multiplyMaps[A](map1: Map[A, Double], map2: Map[A, Double]): Map[A, Double] =
    map1.foldLeft(map1)((acc, i) => acc.updated(i._1, i._2 * map2.getOrElse(i._1, 1.0)))
}

object Spending {
  def apply[A](as: A*): Spending[A] =
    Spending(
      as.foldLeft(Map[A, Double]())((acc, i) => mergeMaps(acc, Map(i -> 1.0)))
    )

  private def mergeMaps[A](map1: Map[A, Double], map2: Map[A, Double]): Map[A, Double] =
    map2.foldLeft(map1)((acc, i) => acc.updated(i._1, i._2 + acc.getOrElse(i._1, 0.0)))
}