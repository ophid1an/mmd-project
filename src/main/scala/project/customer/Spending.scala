package project.customer

case class Spending[A](vec: Map[A, Double]) {
  def cnt: Double = vec.values.sum

  def ++(other: Spending[A]): Spending[A] =
    this.copy(Spending.mergeMaps(vec, other.vec))

  def *(other: Map[A, Double]): Spending[A] = // Is not commutative!!!
    this.copy(multiplyMaps(vec, other))

  def fractional: Spending[A] = this.copy(vec.mapValues(_ / cnt))

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