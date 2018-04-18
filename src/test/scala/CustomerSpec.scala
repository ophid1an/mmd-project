import org.scalatest.FunSuite
import project.customer.Customer

class CustomerSpec extends FunSuite {

  val c0 = Customer(0, Map[String, Double]())
  val c1 = Customer(0, Map("a" -> 3, "b" -> 1))
  val c2 = Customer(0, Map("a" -> 2, "c" -> 4))
  val c3 = Customer(1, Map("a" -> 2, "c" -> 4))

  test("c0.cnt should be 0") {
    assert(c0.cnt == 0)
  }

  test("c0 + \"a\" + \"b\" + \"a\" + \"a\" should be c1") {
    assert(c0 + "a" + "b" + "a" + "a" == c1)
  }

  test("c0 + Array(\"a\", \"b\", \"a\") + Array(\"a\") should be c1") {
    assert(c0 + Array("a", "b", "a") + Array("a") == c1)
  }

  test("c0 + Map(\"a\" -> 3.0, \"b\" -> 1.0) should be c1") {
    assert(c0 + Map("a" -> 3.0, "b" -> 1.0) == c1)
  }

  test("c1.cnt should be 4") {
    assert(c1.cnt == 4)
  }

  test("c1 + \"a\" should be Customer(0, Map(\"a\" -> 4, \"b\" -> 1))") {
    assert(c1 + "a" == Customer(0, Map("a" -> 4, "b" -> 1)))
  }

  test("c1 + (\"a\", 0.1) should be Customer(0, Map(\"a\" -> 3.1, \"b\" -> 1))") {
    assert(c1 + ("a", 0.1) == Customer(0, Map("a" -> 3.1, "b" -> 1)))
  }

  test("c1 + \"c\" should be Customer(0, Map(\"a\" -> 3, \"b\" -> 1, \"c\" -> 1))") {
    assert(c1 + "c" == Customer(0, Map("a" -> 3, "b" -> 1, "c" -> 1)))
  }

  test("c1 + (\"c\", 0.1) should be Customer(0, Map(\"a\" -> 3, \"b\" -> 1, \"c\" -> 0.1))") {
    assert(c1 + ("c", 0.1) == Customer(0, Map("a" -> 3, "b" -> 1, "c" -> 0.1)))
  }

  test("c1 + (\"a\", 0) should produce RunTimeException") {
    assertThrows[RuntimeException] {
      c1 + ("a", 0)
    }
  }

  test("c1 + (\"a\", -0.01) should produce RunTimeException") {
    assertThrows[RuntimeException] {
      c1 + ("a", -0.01)
    }
  }

  test("c1 + Array[String]() should be c1") {
    assert(c1 + Array[String]() == c1)
  }

  test("c1 + Array(\"a\", \"c\") should be Customer(0, Map(\"a\" -> 4, \"b\" -> 1, \"c\" -> 1))") {
    assert(c1 + Array("a", "c") == Customer(0, Map("a" -> 4, "b" -> 1, "c" -> 1)))
  }

  test("c1 + Array(\"a\", \"a\") should be Customer(0, Map(\"a\" -> 5, \"b\" -> 1))") {
    assert(c1 + Array("a", "a") == Customer(0, Map("a" -> 5, "b" -> 1)))
  }

  test("c1 + Map[String, Double]() should be c1") {
    assert(c1 + Map[String, Double]() == c1)
  }

  test("c1 + Map(\"a\" -> 1.0, \"b\" -> 0.1, \"c\" -> 2.0) should be Customer(0, Map(\"a\" -> 4, \"b\" -> 1.1, \"c\" -> 2))") {
    assert(c1 + Map("a" -> 1.0, "b" -> 0.1, "c" -> 2.0) == Customer(0, Map("a" -> 4, "b" -> 1.1, "c" -> 2)))
  }

  test("c1.fractional should be Customer(0, Map(\"a\" -> 0.75, \"b\" -> 0.25)))") {
    assert(c1.fractional == Customer(0, Map("a" -> 0.75, "b" -> 0.25)))
  }

  test("c1 ++ c2 should be Customer(0, Map(\"a\" -> 5, \"b\" -> 1, \"c\" -> 4))") {
    assert(c1 ++ c2 == Customer(0, Map("a" -> 5, "b" -> 1, "c" -> 4)))
  }

  test("c1 ++ c3 should produce RunTimeException") {
    assertThrows[RuntimeException] {
      c1 ++ c3
    }
  }
}