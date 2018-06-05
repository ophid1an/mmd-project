package project

case class Customer[A](clSpending: Spending[A],
                       subClSpending: Spending[A]) {
  def ++(other: Customer[A]): Customer[A] =
    this.copy(clSpending = clSpending ++ other.clSpending,
      subClSpending = subClSpending ++ other.subClSpending)

  def idsToStrings(taxonomy: Taxonomy): Customer[String] =
    this.copy(clSpending = clSpending.toClString(taxonomy),
      subClSpending = subClSpending.toSubClString(taxonomy))

  def fractional: Customer[A] =
    this.copy(clSpending = clSpending.fractional,
      subClSpending = subClSpending.fractional)
}
