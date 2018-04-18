package project.customer

case class Customer[A, B](id: A, vec: Map[B, Double]) {
  lazy val cnt: Double = if (vec.isEmpty) 0 else vec.values.sum

  def +(subCl: B, n: Double = 1): Customer[A, B] =
    if (n > 0) {
      this.copy(vec = mergeVecs(vec, Map(subCl -> n)))
    }
    else sys.error("Customer.+ with an amount less than or equal to zero.")

  def +(seq: Seq[B]): Customer[A, B] =
    this.copy(vec = mergeVecs(vec,
      seq.foldLeft(Map[B, Double]())((acc, i) => mergeVecs(acc, Map(i -> 1.0)))
    ))

  def +(m: Map[B, Double]): Customer[A, B] =
    this.copy(vec = mergeVecs(vec, m))

  def ++(other: Customer[A, B]): Customer[A, B] =
    if (id == other.id) this.copy(vec = mergeVecs(vec, other.vec))
    else sys.error("Customer.++ with a different ID.")

  def fractional: Customer[A, B] = this.copy(vec = vec.mapValues(_ / cnt))

  // Merge two customer vectors grouping by keys and summing their values
  private def mergeVecs[B](vec1: Map[B, Double], vec2: Map[B, Double]): Map[B, Double] =
    vec2.foldLeft(vec1)((acc, i) => acc.updated(i._1, i._2 + acc.getOrElse(i._1, 0.0)))
}

//case class Customer[A, B](id: A, spending: AbsoluteSpending[B]) {
//  def cnt: Double = spending.cnt
//
//  def +(sp: AbsoluteSpending[B]): Customer[A, B] =
//    this.copy(spending = this.spending ++ sp)
//
//  def ++(other: Customer[A, B]): Customer[A, B] =
//    if (id == other.id) this.copy(spending = this.spending ++ other.spending)
//    else sys.error("Customer.++ with a different ID.")
//}
