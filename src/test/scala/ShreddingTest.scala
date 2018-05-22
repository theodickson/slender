//package slender
//
//import slender.dsl.implicits._
//import definitions._
//import org.scalatest.FunSuite
//
//
//class ShreddingTests extends FunSuite {
//
//  val packetType = ProductKeyType(IntKeyType, IntKeyType, StringKeyType)
//  val server = PhysicalBag(
//    ProductKeyType(IntKeyType, BoxedRingType(BagType(packetType)), BoxedRingType(BagType(packetType))),
//    "server"
//  )
//
//  val intPairs = PhysicalBag(ProductKeyType(IntKeyType,IntKeyType), "intPairs")
//
////  test("Packet query") {
////
////    val p = EqualsPredicate(IntKeyExpr(1), IntKeyExpr(1)) //todo
////
////    def invalid(x: String) = For("y" <-- fromK(x._2).dot(fromK(x._3)) iff p) Yield "y"._1._3
////
////    val query = For("x" <-- server) Yield("x"._1, toK(invalid("x")))
////    println(query.shredAndExplain)
////  }
//
//  test("Packet query") {
//
//    val p = EqualsPredicate(IntKeyExpr(1), IntKeyExpr(1)) //todo
//
//    def invalid(in: KeyExpr, out: KeyExpr) = For(("inPkt","outPkt") <-- fromK(in).dot(fromK(out)) iff p) Yield "inPkt"._3
//
//    val query = For(("ip","in","out") <-- server) Yield("ip", toK(invalid("in","out")))
//    println(query.shredAndExplain)
//  }
//
////  test("Key nesting") {
////    def group(k: KeyExpr) = For ("x" <-- intPairs iff (k === "x"._1)) Yield "x"._2
////    val query = For ("k" <-- intPairs) Yield ("k"._1,toK(group("k"._1)))
////    println(query.shredAndExplain)
////  }
//
//  test("Key nesting") {
//    def group(k: KeyExpr) = For (("x1","x2") <-- intPairs iff (k === "x1")) Yield "x2"
//    val query = For (("k1","k2") <-- intPairs) Yield ("k1",toK(group("k1")))
//    println(query.shredAndExplain)
//  }
//
//  //
////  test("Double nesting") {
////    val intTriples = PhysicalBag(ProductKeyType(IntKeyType,IntKeyType,IntKeyType), "intTriples")
////
////    val query =
////      For ("x" <-- intTriples) Yield
////        ("x"._1, toK(
////          For ("y" <-- intTriples iff "y"._1 === "x"._1) Yield
////            ("y"._2, toK(
////              For ("z" <-- intTriples iff "z"._2 === "y"._2) Yield "z"._3
////            )
////            )
////        )
////        )
////    println(query.shredAndExplain)
////  }
//
//
//  test("Double nesting") {
//    val intTriples = PhysicalBag(ProductKeyType(IntKeyType,IntKeyType,IntKeyType), "intTriples")
//
//    val query =
//      For (("x1","x2","x3") <-- intTriples) Yield
//        ("x1", toK(
//          For (("y1","y2","y3") <-- intTriples iff "y1" === "x1") Yield
//            ("y2", toK(
//              For (("z1","z2","z3") <-- intTriples iff "z2" === "y2") Yield "z3"
//            )
//            )
//        )
//        )
//    println(query.shredAndExplain)
//  }
//}