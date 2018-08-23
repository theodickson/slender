package slender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.Duration
import org.scalatest.FunSuiteLike


trait SlenderSparkTest extends FunSuiteLike with TestUtils {
  Logger.getLogger("org").setLevel(Level.ERROR)
}

trait SlenderSparkStreamingTest extends SlenderSparkTest {
  def batchDuration: Duration
}
