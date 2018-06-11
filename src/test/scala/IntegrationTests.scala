package slender

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import scala.collection.immutable.Map

class IntegrationTests extends FunSuite {

  import implicits._

//  val spark = SparkSession.builder()
//    .appName("test")
//    .config("spark.master", "local")
//    .getOrCreate()
//
//  implicit val sc = spark.sparkContext

  val bagOfInts = PhysicalCollection(
    Map(1 -> 1, 2 -> 2, 3 -> 3)
  )

//  test("Nested yield") {
//    val query = For (X <-- bagOfInts) Yield toK(For(Y <-- bagOfInts) Yield Y)
//    assert(query.isResolved)
//    assert(query.isEvaluable)
//  }
//
//  test("Predicated yield test") {
//    val query = For (X <-- bagOfInts iff X === X) Yield X
//    assert(query.isResolved)
//    //query.resolve
//    println(query.eval)
//  }

  test("") {
    val expr1 = Tuple2VariableExpr(TypedVariable[Int](""), TypedVariable[Int](""))
    val expr2 = SumExpr(PhysicalCollection(Set(1,2,3)))
    expr1.resolve
    expr2.resolve
//    println(expr.resolve.eval)
  }
//  test("Simple query") {
//    val query = For(X <-- bagOfInts) Yield X --> NumericExpr(1)
//    assert(query.isResolved)
//    assert(query.isEvaluable)
//    println(query.eval)
//  }

//  test("Full packet query") {
//    val serverData = Map(
//      (1,
//        Map(
//          (1,"1") -> 1,
//          (2,"2") -> 1,
//          (3,"3") -> 1
//        ),
//        Map(
//          (1,"1") -> 1,
//          (3,"3") -> 1,
//          (4,"4") -> 1,
//          (5,"5") -> 1
//        )
//      ) -> 1
//    )
//
//    val server = PhysicalCollection(serverData)
//
//    def invalid[In <: UntypedVariable[In],Out <: UntypedVariable[Out]](in: In, out: Out) =
//      For (((Z1,W1),(Z2,W2)) <-- (toRing(in) dot toRing(out)) iff (Z1 === Z2)) Yield (W1,W2)
//
//    val query = For ((X,Y1,Y2) <-- server) Yield (X,toK(invalid(Y1,Y2)))
//
//    assert(query.isResolved)
//    assert(query.isEvaluable)
////    println(query.eval)
//  }
//
//  test("Simple packet query") {
//    val serverData = Map(
//      (1,
//        Map(
//          (1, "1") -> 1,
//          (2, "2") -> 1,
//          (3, "3") -> 1
//        ),
//        Map(
//          (1, "1") -> 1,
//          (3, "3") -> 1,
//          (4, "4") -> 1,
//          (5, "5") -> 1
//        )
//      ) -> 1
//    )
//
//    val server = PhysicalCollection(serverData)
//
//    def simpleInvalid[In <: UntypedVariable[In], Out <: UntypedVariable[Out]](in: In, out: Out) =
//      For(((Z1,Z2),(W1,W2)) <-- (toRing(in) dot toRing(out)) iff (Z1 === W1)) Yield(Z1,Z2)
//
//    val simpleQuery = For((X,Y1,Y2) <-- server) Yield (X,toK(simpleInvalid(Y1,Y2)))
//
//    assert(simpleQuery.isResolved)
//    assert(simpleQuery.isEvaluable)
//
//  }
//
//  test("Double nested variables") {
//    val data = Map(((1,2),(3,4)) -> 1)
//    val coll = PhysicalCollection(data)
//    val query = For(((Z1,Z2),(W1,W2)) <-- coll) Yield Z1
//    assert(query.isResolved)
//    assert(query.isEvaluable)
//  }
}
