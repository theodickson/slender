package slender

import slender.dsl.implicits._
import definitions._
import org.scalatest.FunSuite


class ShreddingTests extends FunSuite {

  val packetType = ProductKeyType(IntKeyType, IntKeyType, StringKeyType)
  val server = PhysicalBag(
    ProductKeyType(IntKeyType, BoxedRingType(BagType(packetType)), BoxedRingType(BagType(packetType))),
    "server"
  )

  val intPairs = PhysicalBag(ProductKeyType(IntKeyType,IntKeyType), "intPairs")

  test("") {

    val p = EqualsPredicate(IntKeyExpr(1), IntKeyExpr(1)) //todo

    def invalid(x: String) = For("y" <-- fromK(x._2).dot(fromK(x._3)) iff p) Yield "y"._1._3

    val query = For("x" <-- server) Yield("x"._1, toK(invalid("x")))
    println(query.shredAndExplain)
  }

  test("Key nesting") {
    def group(k: KeyExpr) = For ("x" <-- intPairs iff (k === "x"._1)) Yield "x"._2
    val query = For ("k" <-- intPairs) Yield ("k"._1,toK(group("k"._1)))
    println(query.shredAndExplain)
  }

  test("Double nesting") {
    val intTriples = PhysicalBag(ProductKeyType(IntKeyType,IntKeyType,IntKeyType), "intTriples")

    val query =
      For ("x" <-- intTriples) Yield
        ("x"._1, toK(
          For ("y" <-- intTriples iff "y"._1 === "x"._1) Yield
            ("y"._2, toK(
              For ("z" <-- intTriples iff "z"._2 === "y"._2) Yield "z"._3
            )
            )
        )
        )
    println(query.shredAndExplain)
  }
}