package project.customer

case class Customer[A, B](id: A, spending: Spending[B]) {
  def +(spending: Spending[B]): Customer[A, B] =
    this.copy(spending = this.spending ++ spending)

  def ++(other: Customer[A, B]): Customer[A, B] =
    if (id == other.id) this.copy(spending = this.spending ++ other.spending)
    else sys.error("Customer.++ with a different ID.")
}
