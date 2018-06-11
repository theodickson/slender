package slender
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

import scala.reflect.runtime.universe._

object TestUtils {
  def printType[T](t: T)(implicit ev: TypeTag[T]): Unit = println(ev.tpe)
  def getSparkSession: (SparkSession,SparkContext) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("test")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext
    (spark,sc)
  }

  def _assert(t: => Boolean): Unit = if (!t) throw new AssertionError("assertion failed.")
//  def printEval[T, E <: Expr](e: E)(implicit eval: Eval[E,T]): Unit = println(e.eval)
}
