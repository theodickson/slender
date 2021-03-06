package slender

import java.lang.System.currentTimeMillis

import scala.reflect.runtime.universe._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import shapeless.HList

import scala.collection.mutable
import scala.reflect.ClassTag

trait TestUtils {

  def _assert(t: => Boolean): Unit = if (!t) throw new AssertionError("assertion failed.")

  implicit class AnyImplicits[T](x: T) {

    val labelTpe = typeOf[Label[Any,Any]].typeConstructor

    def prettyType[T](t: T)(implicit tag: WeakTypeTag[T]): String = _prettyType(tag.tpe)

    private def _prettyType[T](tpe: Type): String = tpe.typeConstructor match {
      case x if (x == labelTpe) => {
        val idType = tpe.typeArgs.head
        val id = _prettyType(idType).hashCode.abs.toString.take(3)
        s"Label[$id]"
      }
      case y => {
        val const = y.toString.split('.').last
        val args = tpe.typeArgs.map(_prettyType(_)).mkString(", ")
        if (tpe.typeArgs.isEmpty) const
        else if (const.startsWith("Tuple")) s"($args)"
        else s"$const[$args]"
      }
    }

    def printType(implicit tag: WeakTypeTag[T]): Unit = println(prettyType(tag))

  }

  implicit class RddTestImplicits[T](rdd: RDD[T]) {

    def dataToString: String = rdd.take(10).map(_.toString).mkString("\n")
    def printData: Unit = print(dataToString)
    def profile(name: String, toCache: List[RDD[_]]): Unit = {
      toCache.foreach { r => r.cache.count }
      val startTime = currentTimeMillis()
      rdd.count
      val timeTaken = currentTimeMillis - startTime
      println(s"$name: $timeTaken")
    }

  }

  implicit class DStreamTestImplicits[K:ClassTag,V:ClassTag](dstream: DStream[(K,V)])
                                                            (implicit ring: Ring[V]) extends Serializable {
    def equalsRdd(rdd: RDD[(K,V)]): DStream[Boolean] = {
      dstream.transform[Boolean] { rdd1: RDD[(K,V)] =>
        rdd.reduceByKey(ring.add).fullOuterJoin(rdd1.reduceByKey(ring.add))
          .map { case (_,(r1Opt,r2Opt)) => ((),r1Opt.fold(false)(r1 => r2Opt.fold(false)(r2 => ring.equiv(r1,r2))))}
          .reduceByKey(_ && _).map(_._2)
      }
    }
  }

  implicit class SDStreamImplicits[DS <: SDStream[DS],T](sdstream: SDStream.Aux[DS,T]) {
    def profile(limit: Int = 10, toCache: List[RDD[_]], spark: SparkSession, ssc: StreamingContext): Unit = {
      val dstream = sdstream.dstream
      toCache.foreach { rdd => rdd.cache.count }
      var counter: Int = 0
      var time = currentTimeMillis()
      var done: Boolean = false
      var totalTime: Long = 0
      dstream.foreachRDD { rdd =>
        if (!done) {
          counter += 1
          val count = rdd.count
          val currentTime = currentTimeMillis()
          val interval = currentTime - time
          totalTime += interval
          //println(s"$counter - $totalTime ($interval) ($count)")
          time = currentTime
          if (counter == limit) {println(s"$counter - $totalTime ($interval) ($count)"); done = true}
        }
      }
      ssc.start()
      while(!done) { java.lang.Thread.sleep(100) }
      ssc.stop()
      spark.stop()
    }
  }

  implicit class ShreddedResultImplicits[DS <: SDStream[DS],T,Ctx <: HList]
  (result: ShreddedResult[SDStream.Aux[DS,T],Ctx]) {
    def profile(limit: Int = 10, toCache: List[RDD[_]],
                acc: Boolean = false, spark: SparkSession, ssc: StreamingContext)
               (implicit accumulate: Accumulate[DS,T]): Unit = {
      val dstream = if (acc) result.flat.acc.dstream else result.flat.dstream
      toCache.foreach { rdd => rdd.cache.count }
      var counter: Int = 0
      var time = currentTimeMillis()
      var done: Boolean = false
      var totalTime: Long = 0
      dstream.foreachRDD { rdd =>
        if (!done) {
          counter += 1
          val count = rdd.count
          val currentTime = currentTimeMillis()
          val interval = currentTime - time
          totalTime += interval
          //println(s"$counter - $totalTime ($interval) ($count)")
          time = currentTime
          if (counter == limit) {println(s"$counter - $totalTime ($interval) ($count)"); done = true}
        }
      }
      ssc.start()
      while(!done) { java.lang.Thread.sleep(100) }
      ssc.stop()
      spark.stop()
    }
  }

  def rddToDStream[T:ClassTag](rdd: RDD[T], n: Int = 5, ssc: StreamingContext): DStream[T] = {
    val rddWithIds: RDD[(T,Long)] = rdd.zipWithUniqueId
    val queue = new mutable.Queue[RDD[T]]
    (0 until n).map(i => rddWithIds.filter(_._2 % n == i))
      .foreach { r =>
        val toAdd = r.map(_._1)
        toAdd.cache.count
        queue.enqueue(toAdd)
      }
    ssc.queueStream(queue, true)
  }
}