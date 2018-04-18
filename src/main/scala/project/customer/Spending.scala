package project.customer

case class Spending[A](vec: Map[A, Double]) {
  def cnt: Double = vec.values.sum

  def ++(other: Spending[A]): Spending[A] =
    this.copy(mergeMaps(vec, other.vec))

  def *(other: Map[A, Double]): Spending[A] =
    this.copy(vec.keys.toList.map(k => k -> (vec.getOrElse(k, 0.0) * other.getOrElse(k, 1.0))).toMap)

  def fractional: Spending[A] = this.copy(vec.mapValues(_ / cnt))

  private def mergeMaps[A](map1: Map[A, Double], map2: Map[A, Double]): Map[A, Double] =
    map2.foldLeft(map1)((acc, i) => acc.updated(i._1, i._2 + acc.getOrElse(i._1, 0.0)))
}

object Spending {
  def apply[A](as: A*): Spending[A] = as.toList match {
    case Nil => Spending(Map[A, Double]())
    case h :: t => Spending(Map(h -> 1.0)) ++ apply(t: _*)
  }
}