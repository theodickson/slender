package slender

import slender.dsl.implicits._
//import slender.dsl.inference._
import org.scalatest.FunSuite

//  val stringCounts1 = PhysicalCollection(StringKeyType, IntType, "stringCounts1")
//  val stringCounts2 = PhysicalCollection(StringKeyType, IntType, "stringCounts2")
//  val bagOfBags = PhysicalBag(BoxedRingType(MappingType(StringKeyType, IntType)), "bagOfBags")
//  val intCounts = PhysicalCollection(IntKeyType, IntType, "intCounts")
//  val bagOfPairs = PhysicalBag(KeyPairType(StringKeyType,IntKeyType), "bagOfPairs")
//  val const = IntExpr(1)


class ShreddingTests extends FunSuite {

  val packetType = KeyTuple3Type(IntKeyType, IntKeyType, StringKeyType)
  val server = PhysicalBag(
    KeyTuple3Type(IntKeyType, BoxedRingType(BagType(packetType)), BoxedRingType(BagType(packetType))),
    "server"
  )

  val intPairs = PhysicalBag(KeyPairType(IntKeyType,IntKeyType), "intPairs")

  test("") {

    val p = Predicate(IntKeyExpr(1), IntKeyExpr(1)) //todo

    def invalid(x: String) = For("y" <-- fromK(x._2).dot(fromK(x._3)) iff p) Yield "y"._1._3

    val query = For("x" <-- server) Yield("x"._1, toK(invalid("x")))
    val shredded = query.shred
    println(query.shredAndExplain)
  }

  test("Key nesting") {
    def group(k: KeyExpr) = For ("x" <-- intPairs iff (k === "x"._1)) Yield "x"._2
    val query = For ("k" <-- intPairs) Yield ("k"._1,toK(group("k"._1)))
    val shredded = query.shred
    println(query.shredAndExplain)
  }

  test("Double nesting") {
    val intTriples = PhysicalBag(KeyTuple3Type(IntKeyType,IntKeyType,IntKeyType), "intTriples")

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
    val shredded = query.shred
    println(query.shredAndExplain)
  }
}