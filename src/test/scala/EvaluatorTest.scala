package slender

import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession


class EvaluatorTests extends FunSuite {

//    val spark = SparkSession.builder
//      .appName("test")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    implicit val sc = spark.sparkContext

  val stringCounts1 = PhysicalCollection(
    Map("a" -> 1, "b" -> 2, "c" -> 3)
  )

  val stringCounts2 = PhysicalCollection(
    Map("a" -> 2, "b" -> 4, "c" -> 6)
  )

  val bagOfIntPairs = PhysicalCollection(
    Map(
      (1, 1) -> 1,
      (1, 2) -> 2,
      (2, 2) -> 3
    )
  )

  val bagOfIntPairPairs = PhysicalCollection(
    Map(
      ((1,1),(1,1)) -> 1
    )
  )

  //val stringCountsRdd = PhysicalCollection(PairRDD(sc.parallelize(List("a" -> 1, "b" -> 2, "c" -> 3))))

//  test("Tuple key test") {
//      val expr = Tuple2KeyExpr(IntKeyExpr(1),IntKeyExpr(1))
//      println(expr.eval)
////    val intExpr = IntExpr(1)
////    println(intExpr.eval)
////
////    val inf = InfiniteMappingExpr(Variable[String](""), intExpr)
////    println(inf.eval)
////
////    val mult = MultiplyExpr(stringCounts1, inf)
////
////    println(mult.eval)
////
////    val collectQuery = SumExpr(mult)
////    println(collectQuery.eval)
////
////    val yieldQuery = SumExpr(MultiplyExpr(stringCounts1, InfiniteMappingExpr(Variable[String]("x"),
////      SngExpr(Variable[String]("x"), IntExpr(1))
////    )))
////
////    println(yieldQuery.eval)
//  }

  test("Tuple variable eval") {
//    val varExpr = Tuple2VariableExpr(Tuple2VariableExpr(Variable[Int]("x"), Variable[Int]("y")),Tuple2VariableExpr(Variable[Int]("x"), Variable[Int]("y")))
//    assertThrows[NoSuchElementException](varExpr.eval)
//    val infiniteMappingExpr = InfiniteMappingExpr(TypedVariable[X,Int](X),IntExpr(1))
//    println(infiniteMappingExpr.eval)//[Int => Int](InfiniteMappingEval[X,IntExpr,Int,Int](VariableEval[X,Int],PrimitiveExprEval[IntExpr,Int]))
//    val varExpr = TypedVariable[X,String](X)
//    val varExpr2 = Tuple2VariableExpr(TypedVariable[X,Int](X),TypedVariable[Y,Int](Y))
//    val expr = SumExpr(MultiplyExpr(stringCounts1, InfiniteMappingExpr(varExpr,SngExpr(varExpr,IntExpr(1)))))
//    println(expr.eval)//SumExpr(MultiplyExpr(stringCounts1, InfiniteMappingExpr1(varExpr,SngExpr(varExpr,IntExpr(1))))).eval
  }

  test("") {
////    val varExpr = TypedVariable[String]("x")
////    val varExpr2 = Tuple2VariableExpr(TypedVariable[Int]("x"), TypedVariable[Int]("y"))
//    val varExpr3 = Tuple2VariableExpr(
//      Tuple2VariableExpr(TypedVariable[X,Int](X),TypedVariable[Y,Int](Y)),
//      Tuple2VariableExpr(TypedVariable[Z,Int](Z), TypedVariable[W,Int](W))
//    )
////    val expr = SumExpr(MultiplyExpr(stringCounts1, InfiniteMappingExpr(varExpr,SngExpr(varExpr,IntExpr(1)))))
////    val expr2 = SumExpr(MultiplyExpr(bagOfIntPairs, InfiniteMappingExpr(varExpr2,SngExpr(varExpr2,IntExpr(1)))))
//    val expr3 = SumExpr(MultiplyExpr(bagOfIntPairPairs, InfiniteMappingExpr(varExpr3,SngExpr(varExpr3,IntExpr(1)))))
////    expr.eval
//    println(expr3.eval)
  }
//
//  test("Project test") {
//    val expr = Project1KeyExpr(Tuple2KeyExpr(IntKeyExpr(1),IntKeyExpr(1)))
//    println(expr.eval)
//  }

//  test("Nested dot test") {
//    val query = DotExpr(DotExpr(stringCounts1,stringCounts2),stringCounts1)
//    println(query.eval)
//  }
//
//  test("Add test") {
//    val query = stringCounts1 + stringCounts2
//    println(query.eval)
//  }
//
//  test("Inf mapping test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCounts1,
//        InfiniteMappingExpr(
//          Variable[String]("x"),IntExpr(2)
//        )
//      )
//    )
//    println(query.eval)
//  }
//
//  test("Rdd*inf mapping test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCountsRdd,
//        InfiniteMappingExpr(
//          Variable[String]("x"),IntExpr(2)
//        )
//      )
//    )
//    println(query.eval)
//  }
//
//  test("Rdd*rdd test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCountsRdd,
//        stringCountsRdd
//      )
//    )
//    println(query.eval)
//  }
//
//  test("Rdd*map test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCountsRdd,
//        stringCounts1
//      )
//    )
//    println(query.eval)
//  }
  ////
//  test("Shredding test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        bagOfIntPairs,
//        InfiniteMappingExpr(
//          Variable[(Int, Int)]("x"),
//          SngExpr(
//            BoxedRingExpr(
//              MultiplyExpr(
//                bagOfIntPairs,
//                InfiniteMappingExpr(
//                  Variable[(Int, Int)]("y"),
//                  SngExpr(Variable[(Int, Int)]("y"), IntExpr(1))
//                )
//              )
//            ), IntExpr(1)
//          )
//        )
//      )
//    )
//    println(query.eval)
//    println(query.shred.eval)
//    //    println(query.shred.eval)
//    //  }
//
//  }
}
