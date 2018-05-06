package slender

import slender.dsl.implicits._
import slender.dsl.inference._
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

  test("") {
    val query = For ("x" <-- server) Yield (

      (
        "x"._1,
        toK(
          For ("y" <-- fromK("x"._2).dot(fromK("x"._3))) Yield "y"._1._3
        )
      )
    )

    println(query)
    println(query.ringType)

    val shredded = query.shred
    println(shredded)
    println(shredded.ringType)
  }
}