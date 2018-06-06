package project

case class Customer[A](clSpending: Spending[A],
                       subClSpending: Spending[A]) {
  def ++(other: Customer[A]): Customer[A] =
    this.copy(clSpending = clSpending ++ other.clSpending,
      subClSpending = subClSpending ++ other.subClSpending)

  def fractional: Customer[A] =
    this.copy(clSpending = clSpending.fractional,
      subClSpending = subClSpending.fractional)
}
