package project.customer

sealed trait Spending[A]

object Spending {

  case class AbsoluteSpending[A](vec: Map[A, Double]) extends Spending[A] {
    def ++(other: AbsoluteSpending[A]): AbsoluteSpending[A] =
      this.copy(mergeMaps(vec, other.vec))

    def cnt: Double = vec.values.sum

    def fractional: FractionalSpending[A] = FractionalSpending[A](vec.mapValues(_ / cnt))
  }

  case class FractionalSpending[A](vec: Map[A, Double]) extends Spending[A]

  def apply[A](as: A*): AbsoluteSpending[A] =
    AbsoluteSpending(
      as.foldLeft(Map[A, Double]())((acc, i) => mergeMaps(acc, Map(i -> 1.0)))
    )

  private def mergeMaps[A](map1: Map[A, Double], map2: Map[A, Double]): Map[A, Double] =
    map2.foldLeft(map1)((acc, i) => acc.updated(i._1, i._2 + acc.getOrElse(i._1, 0.0)))
}