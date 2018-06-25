package slender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuiteLike

import scala.reflect.runtime.universe._

trait TestUtils {

  def printType[T](t: T)(implicit ev: TypeTag[T]): Unit = println(ev.tpe)

//  def printRdd[T](t: RDD[T])(implicit ss: SparkSession): Unit = {
//    import ss.implicits._
//    t.toDF().show
//  }

  def _assert(t: => Boolean): Unit = if (!t) throw new AssertionError("assertion failed.")

}

trait SlenderTest extends FunSuiteLike with TestUtils

trait SlenderSparkTest extends SlenderTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark = SparkSession.builder
    .appName("test")
    .config("spark.master", "local")
    .getOrCreate()

  implicit val sc = spark.sparkContext
}
