package slender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}
import org.scalatest.FunSuiteLike

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


trait TestUtils {

 // def printType[T](t: T)(implicit ev: TypeTag[T]): Unit = println(ev.tpe)

//  def printRdd[T](t: RDD[T])(implicit ss: SparkSession): Unit = {
//    import ss.implicits._
//    t.toDF().show
//  }

  //Massive type trees seem to bork the built-in assertion
  def _assert(t: => Boolean): Unit = if (!t) throw new AssertionError("assertion failed.")

}

trait SlenderTest extends FunSuiteLike with TestUtils

trait SparkTestImplicits {
//  implicit class RddTestImplicits[K:ClassTag,V:ClassTag](rdd: RDD[(K,V)]) {
//
////    implicit def unsafeMapOrdering[K,V]: Ordering[Map[K,V]] = new Ordering[Map[K,V]] {
////      def compare(x: Map[K,V], y: Map[K,V]): Int = ???
////    }
//
//    def print: Unit = rdd.take(10).foreach(println)
////    def ~=(other: RDD[(K,V)])(implicit ord: Ordering[K]): Boolean = {
////      val c1 = rdd.count; val c2 = other.count
////      if (c1 == c2) {
////        rdd.sortByKey(ascending=true).zip(other.sortByKey(ascending=true)).map { case (x,y) => x == y } reduce (_ && _)
////      } else false
////    }
//  }
}
trait SlenderSparkTest extends SlenderTest with SparkTestImplicits {
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark = SparkSession.builder
    .appName("test")
    .config("spark.master", "local")
    .getOrCreate()

  implicit val sc = spark.sparkContext

  //def printRdd[T](rdd: RDD[T]): Unit = rdd.take(10).foreach(println)
}

trait SlenderSparkStreamingTest extends SlenderSparkTest {
  def batchDuration: Duration
  implicit lazy val ssc = {
    val out = new StreamingContext(sc,batchDuration)
    out.checkpoint("_checkpoint")
    out
  }
}
