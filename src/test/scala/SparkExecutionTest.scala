package slender

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder}
import slender.execution._
import slender.execution.implicits._
import slender.dsl.implicits._
import slender.definitions._

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

import scala.reflect.runtime.universe._
import org.scalatest.FunSuite

object TestSparkExecutionContext extends LocalSparkExecutionContext {
  import spark.implicits._
  val sc = spark.sparkContext
//  val stringCounts = PhysicalCollection(
//    StringKeyType,IntType,"stringCounts",true
//  )

  val stringCounts =
    Map(
      ("a",1),
      ("b",2),
      ("c",3)
    ).toList.toDS

  val nestedBag = Map(
    Map("a" -> 1, "b" -> 2) -> 3,
    Map("a" -> 2, "c" -> 4) -> 4
  ).toList.toDS
}


object dsimplicits {

  implicit class DatasetImplicits(ds: Dataset[_]) {
    def collectAsMap[K,V](implicit enc: Encoder[(K,V)]): Map[K,V] = ds.as[(K,V)].collect.toMap

    def testEquals[K,V](m: Map[K,V])(implicit enc: Encoder[(K,V)]): Boolean =
      collectAsMap == m

    def assertEquals[K,V](m: Map[K,V])(implicit enc: Encoder[(K,V)]): Unit =
      assert(ds.testEquals(m))
  }

  implicit class DatasetPair[K,V](ds: Dataset[(K,V)]) {
    def reduceByKey(op: (V,V) => V)(implicit ev1: Encoder[K], ev2: Encoder[V], ev3: Encoder[(K,V)]): Dataset[(K,V)] =
      ds.groupByKey(_._1).reduceGroups { (kv1: (K,V), kv2: (K,V)) =>
        (kv1._1,op(kv1._2,kv2._2))
      } map (kkv => (kkv._1,kkv._2._2))
  }

  def dsprint(a: Any): Unit = a match {
    case ds: Dataset[_] => ds.show
    case _ => println(a)
  }

}

class SparkExecutionTests extends FunSuite {

  val ctx = TestSparkExecutionContext

  import ctx._
  import dsimplicits._
  import spark.implicits._

  val interpreter = LocalInterpreter(TestSparkExecutionContext)


//  test("Retrieve list collection unchanged.") {
//    val result = interpreter(stringCounts)
//    dsprint(result)
//    result.asInstanceOf[Dataset[_]] assertEquals
//      Map(
//        ("a",1),
//        ("b",2),
//        ("c",3)
//      )
//  }

//  test("Sum simple bag.") {
//    val result = interpreter.apply(Sum(stringCounts))
//    dsprint(result)
//  }

//  ignore("Flatten test") {
//    val result = interpreter(nestedBag)
//    val manuallyFlattened = nestedBag.flatMap {
//      case (bag,v) => bag.mapValues(_ * v)
//    }.reduceByKey(_ + _)
//
//    val flattenQuery = For ("x" <-- nestedBag) Collect "x"
//    println(flattenQuery.explain)
//    val innerQuery = Multiply(nestedBag, InfiniteMappingExpr("x","x")).inferTypes
//    println(innerQuery.explain)
//    val automaticallyFlattened = interpreter(flattenQuery)
//    dsprint(result)
//    dsprint(manuallyFlattened)
//    println(interpreter.showProgram(innerQuery))
//    dsprint(interpreter(innerQuery))
//
//  }
//
//  test("Simple dataset map test") {
//    val mapProgram = q"""
//    val spark = org.apache.spark.sql.SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
//    import spark.implicits._
//    val stringCounts = Map(
//      ("a",1),
//      ("b",2),
//      ("c",3)
//    ).toList.toDS
//    stringCounts.map(x => x).show
//    """
//    interpreter.executor(mapProgram)
//    //    dsprint(automaticallyFlattened)
//  }

  test("Spark macro test") {
    val result = sparkTestMacro
    result.asInstanceOf[Dataset[_]].show
  }

//  test("Sum mapping to nested pairs.") {
//    val result = interpreter.apply(Sum(stringIntNestedPairs))
//    assert(result == ((6,6),(6,6)))
//  }
//
//  test("Add two mappings, then sum") {
//    val expr = Sum(Add(stringIntPairs,stringIntPairs))
//    val result = interpreter.apply(expr)
//    assert(result == (12,12)) //(2,2) + (4,4) + (6,6)
//  }
//
//  test("Simple dot test") {
//    val expr = Dot(stringCounts, IntExpr(2))
//    val result = interpreter.apply(expr)
//    assert(result == Map(
//      ("a",2),
//      ("b",4),
//      ("c",6)
//    ))
//  }
//
//  test("Multiplication test") {
//    val expr = Multiply(stringCounts, nestedStringBag)
//    val result = interpreter.apply(expr)
//    assert(result == Map(
//      ("a", Map()), //todo
//      ("b", Map("b" -> 4)),
//      ("c", Map("c" -> 6))
//    ))
//  }
//
//  test("Inf mapping simple test") {
//    val expr = For ("x" <-- stringCounts) Collect 2
//    val result = interpreter(expr)
//    assert(result == 12)
//  }
//
//  test("Inf mapping nested test") {
//    val expr = For ("x" <-- nestedStringBag) Collect 3
//    val result = interpreter(expr)
//    assert(result == Map("b" -> 6, "c" -> 6, "d" -> 6))
//  }
//
//  test("Nested tuple yield test") {
//    val expr = For ("x" <-- stringIntNestedPairs) Yield "x"
//    val result = interpreter(expr)
//    assert(result == stringIntNestedPairs)
//  }
//
//  test("Map-valued yield test") {
//    val expr = For ("x" <-- nestedStringBag) Yield "x"
//    val result = interpreter(expr)
//  }
//
//  test("Filtering test") {
//    val query = For ("x" <-- stringCounts iff "x" === StringKeyExpr("a")) Yield "x"
//    val result = interpreter(query)
//    assert(result == Map("a" -> 1, "b" -> 0, "c" -> 0))
//  }
//
//  test("Group test") {
//    val group: KeyExpr => RingExpr = k =>
//      For (("k1","k2") <-- bagOfPairs iff k === "k1") Yield "k2"
//    val query = toK(group(StringKeyExpr("a")))
//    val result = interpreter(query)
//  }
//
//
//  test("Double key-nesting test") {
//    val query =
//      For(("x1", "x2", "x3") <-- bagOfTriples) Yield
//        ("x1", toK(
//          For(("y1", "y2", "y3") <-- bagOfTriples iff "y1" === "x1") Yield
//            ("y2", toK(
//              For(("z1", "z2", "z3") <-- bagOfTriples iff "z2" === "y2") Yield "z3"
//            )
//            )
//        )
//        )
//    assert(interpreter(query) == interpreter(query.shred.renest))
//    //    println(query.explain)
//    //    println(query.shred.explain)
//    //    println(query.shred.renest.explain)
//    //    println(interpreter(query))
//    //    println(interpreter(query.shred))
//    //    println(interpreter(query.shred.renest))
//  }

  //spark.stop
}
