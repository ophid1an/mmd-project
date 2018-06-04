import org.scalatest.FunSuite
import project.Taxonomy

class TaxonomySpec extends FunSuite {

  val t0 = Taxonomy()
  val t1 = Taxonomy("a", List("a1"))
  val t2 = Taxonomy("b", List("b1"))
  val t3 = Taxonomy("c", List("b1", "b2"))
  val t1_2 = Taxonomy(Map("a" -> 0, "b" -> 1), Map("a1" -> 0, "b1" -> 1), Map("a1" -> 0, "b1" -> 1),
    Map(0 -> 0, 1 -> 1), Map(0 -> 0, 1 -> 1))
  val t1_2_3 = Taxonomy(Map("a" -> 0, "b" -> 1, "c" -> 2), Map("a1" -> 0, "b1" -> 1),
    Map("a1" -> 0, "b1" -> 1, "b2" -> 2), Map(0 -> 0, 1 -> 1, 2 -> 2), Map(0 -> 0, 1 -> 1, 2 -> 1))

  test("t0.ProductsToSubClasses") {
    assert(t0.productsToSubClasses == Map())
  }

  test("t0.subClassesToClasses") {
    assert(t0.subClassesToClasses == Map())
  }

  test("t0.subClassesToProducts") {
    assert(t0.subClassesToProducts == Map())
  }

  test("t0.classesToSubClasses") {
    assert(t0.classesToSubClasses == Map())
  }

  test("t1_2.subClassesToProducts") {
    assert(t1_2.subClassesToProducts == Map(0 -> List(0), 1 -> List(1)))
  }

  test("t1_2.classesToSubClasses") {
    assert(t1_2.classesToSubClasses == Map(0 -> List(0), 1 -> List(1)))
  }

  test("t1_2_3.subClassesToProducts") {
    assert(t1_2_3.subClassesToProducts == Map(0 -> List(0), 1 -> List(1), 2 -> List(2)))
  }

  test("t1_2_3.classesToSubClasses") {
    assert(t1_2_3.classesToSubClasses == Map(0 -> List(0), 1 -> List(1, 2)))
  }

  test("t0 ++ t1 == t1") {
    assert(t0 ++ t1 == t1)
  }

  test("t1 ++ t1 == t1") {
    assert(t1 ++ t1 == t1)
  }

  test("t1 ++ t2 == t1_2") {
    assert(t1 ++ t2 == t1_2)
  }

  test("t1_2 ++ t3 == t1_2_3") {
    assert(t1_2 ++ t3 == t1_2_3)
  }

  test("t1 ++ t1_2 should produce RunTimeException") {
    assertThrows[RuntimeException] {
      t1 ++ t1_2
    }
  }
}

