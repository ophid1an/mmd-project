//import org.scalatest.FunSuite
//import project.customer.{Customer, Spending}
//
//class CustomerSpec extends FunSuite {
//
//  val c0 = Customer(0, Spending(Map[String, Double]()))
//  val c1 = Customer(0, Spending(Map("a" -> 3.0, "b" -> 1.0)))
//  val c2 = Customer(0, Spending(Map("a" -> 2.0, "c" -> 4.0)))
//  val c3 = Customer(1, Spending(Map("a" -> 2.0, "c" -> 4.0)))
//
//  test("c0.spending.cnt should be 0") {
//    assert(c0.spending.cnt == 0)
//  }
//
//  test("c0 + project.customer.Spending(\"a\", \"b\", \"a\", \"a\") should be c1") {
//    assert(c0 + Spending("a", "b", "a", "a") == c1)
//  }
//
//  test("c0 + project.customer.Spending(\"a\", \"b\", \"a\") + project.customer.Spending(\"a\") should be c1") {
//    assert(c0 + Spending("a", "b", "a") + Spending("a") == c1)
//  }
//
//  test("c0 + project.customer.Spending(Map(\"a\" -> 3.0, \"b\" -> 1.0) should be c1") {
//    assert(c0 + Spending(Map("a" -> 3.0, "b" -> 1.0)) == c1)
//  }
//
//  test("c1.spending.cnt should be 4") {
//    assert(c1.spending.cnt == 4)
//  }
//
//  test("c1 + project.customer.Spending(\"a\") should be Customer(0, project.customer.Spending(Map(\"a\" -> 4.0, \"b\" -> 1.0)))") {
//    assert(c1 + Spending("a") == Customer(0, Spending(Map("a" -> 4.0, "b" -> 1.0))))
//  }
//
//
//  test("c1 + \"c\" should be project.customer.Spending(Map(\"a\" -> 3, \"b\" -> 1, \"c\" -> 1)))") {
//    assert(c1 + Spending("c") == Customer(0, Spending(Map("a" -> 3.0, "b" -> 1.0, "c" -> 1.0))))
//  }
//
//
//  test("c1 + project.customer.Spending(Array[String](): _*) should be c1") {
//    assert(c1 + Spending(Array[String](): _*) == c1)
//  }
//
//  test("c1 + project.customer.Spending(Array(\"a\", \"c\"): _*) should be Customer(0, project.customer.Spending(Map(\"a\" -> 4.0, \"b\" -> 1.0, \"c\" -> 1.0)))") {
//    assert(c1 + Spending(Array("a", "c"): _*) == Customer(0, Spending(Map("a" -> 4.0, "b" -> 1.0, "c" -> 1.0))))
//  }
//
//  test("c1 + project.customer.Spending(Array(\"a\", \"a\"): _*) should be Customer(0, project.customer.Spending(Map(\"a\" -> 5.0, \"b\" -> 1.0)))") {
//    assert(c1 + Spending(Array("a", "a"): _*) == Customer(0, Spending(Map("a" -> 5.0, "b" -> 1.0))))
//  }
//
//  test("c1 + project.customer.Spending(Map[String, Double]()) should be c1") {
//    assert(c1 + Spending(Map[String, Double]()) == c1)
//  }
//
//  test("c1 + project.customer.Spending(Map(\"a\" -> 1.0, \"b\" -> 0.1, \"c\" -> 2.0)) should be Customer(0, project.customer.Spending(Map(\"a\" -> 4.0, \"b\" -> 1.1, \"c\" -> 2.0)))") {
//    assert(c1 + Spending(Map("a" -> 1.0, "b" -> 0.1, "c" -> 2.0)) == Customer(0, Spending(Map("a" -> 4.0, "b" -> 1.1, "c" -> 2.0))))
//  }
//
//  test("c1.spending.fractional should be project.customer.Spending(Map(\"a\" -> 0.75, \"b\" -> 0.25))") {
//    assert(c1.spending.fractional == Spending(Map("a" -> 0.75, "b" -> 0.25)))
//  }
//
//  test("c1 ++ c2 should be Customer(0, project.customer.Spending(Map(\"a\" -> 5.0, \"b\" -> 1.0, \"c\" -> 4.0)))") {
//    assert(c1 ++ c2 == Customer(0, Spending(Map("a" -> 5.0, "b" -> 1.0, "c" -> 4.0))))
//  }
//
//  test("c1 ++ c3 should produce RunTimeException") {
//    assertThrows[RuntimeException] {
//      c1 ++ c3
//    }
//  }
//}
