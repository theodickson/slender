package slender

import slender.dsl.implicits._
import org.scalatest.FunSuite

class PrintingTests extends FunSuite {

  val packetType = KeyTuple3Type(IntKeyType, IntKeyType, StringKeyType)
  val server = PhysicalBag(
    KeyTuple3Type(IntKeyType, BoxedRingType(BagType(packetType)), BoxedRingType(BagType(packetType))),
    "server"
  )

  test("Sum") {
    val p = Predicate(IntKeyExpr(1), IntKeyExpr(1)) //todo

    def invalid(x: String) = For ("y" <-- fromK(x._2).dot(fromK(x._3)) iff p) Yield "y"._1._3

    val query = For ("x" <-- server) Yield ("x"._1, toK(invalid("x")))

    println(query)
    println(query.shred)
  }
}