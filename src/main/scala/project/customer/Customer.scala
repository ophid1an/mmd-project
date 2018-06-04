package project.customer

import project.product.Taxonomy

case class Customer[A, B](id: A, clSpending: Spending[B], subClSpending: Spending[B]) {
  //  def +(spending: Spending[B]): Customer[A, B] =
  //    this.copy(spending = this.spending ++ spending)

  def ++(other: Customer[A, B]): Customer[A, B] =
    if (id == other.id)
      this.copy(clSpending = clSpending ++ other.clSpending,
        subClSpending = subClSpending ++ other.subClSpending)
    else
      sys.error("Customer.++ with a different ID.")

  def idsToStrings(taxonomy: Taxonomy): Customer[A, String] =
    this.copy(clSpending = clSpending.toClString(taxonomy),
      subClSpending = subClSpending.toSubClString(taxonomy))

  def fractional: Customer[A, B] =
    this.copy(clSpending = clSpending.fractional,
      subClSpending = subClSpending.fractional)
}
