package slender

import implicits._
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.{SQLContext, SparkSession}
//
//class Tests extends FunSuite with BeforeAndAfter {
//
//  val sparkConf: SparkConf = new SparkConf()
//  val spark = SparkSession.builder()
//    .config(sparkConf)
//    .master("local[*]")
//    .getOrCreate()
//  implicit val sqlContext = spark.sqlContext
//  import sqlContext.implicits._
//
//  after {
//    spark.stop()
//  }
//
//  val namesList1 = List("Alice", "Bob", "Carl")
//  val namesList2 = List("Dan", "Ed", "Fran")
//
//  val namesDf1 = namesList1.toDF
//  val namesDf2 = namesList2.toDF
//
//  val namesPhysColl1 = DataFrameCollection(namesDf1, "names1")
//  val namesPhysColl2 = DataFrameCollection( namesDf2, "names2")
//
//  val namesLogColl1 = SimpleCollection(StringType, "names1")
//  val namesLogColl2 = SimpleCollection(StringType, "names2")
//
//  test("simple collection resolves successfully") {
//    namesLogColl1.assertPhysicalCollectionsValid(namesPhysColl1)
//  }
//
//  test("simple collection fails with mis-matched refs") {
//    assertThrows[IllegalArgumentException](
//      namesLogColl1.assertPhysicalCollectionsValid(namesPhysColl2)
//    )
//  }
//
//  test("simple collection fails with mis-matched types") {
//    val wrongTypeLogColl = SimpleCollection(IntegerType, "names1")
//    assertThrows[IllegalArgumentException](
//      wrongTypeLogColl.assertPhysicalCollectionsValid(namesPhysColl1)
//    )
//  }
//
//  test("added collections resolve successfully") {
//    val added = Plus(namesLogColl1, namesLogColl2)
//    added.assertPhysicalCollectionsValid(namesPhysColl1, namesPhysColl2)
//  }
//
//  test("simple collection executes successfully") {
//    val result = DataFrameExecutor.execute(namesLogColl1, namesPhysColl1)
//    assert(result <=> namesList1)//result.as[String].collect.toList == namesList1.as[String].collect.toList)
//  }
//
//  test("adding collections executes successfully") {
//    val added = Plus(namesLogColl1, namesLogColl2)
//    val result = DataFrameExecutor.execute(added, namesPhysColl1, namesPhysColl2)
//    assert(result <~> namesList1 ++ namesList2)
//  }
//}
