package slender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.Duration
import org.scalatest.FunSuiteLike


trait TestUtils {
  def _assert(t: => Boolean): Unit = if (!t) throw new AssertionError("assertion failed.")
}

trait SlenderSparkTest extends FunSuiteLike {
  Logger.getLogger("org").setLevel(Level.ERROR)
}

trait SlenderSparkStreamingTest extends SlenderSparkTest {
  def batchDuration: Duration
}
