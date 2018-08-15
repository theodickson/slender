import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import java.lang.System.currentTimeMillis

trait types {
  type Namespace = Map[String,Any]
}

package object slender extends types with Serializable {

  object dsl extends Syntax

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

  implicit def SDStreamToDStream[S <: SDStream[S]](s: S): DStream[s.T] = s.dstream

  implicit class AnyImplicits[T](x: T) {

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

  implicit class DStreamTestImplicits[K:ClassTag,V:ClassTag](dstream: DStream[(K,V)])(implicit ring: Ring[V]) extends Serializable {
    def equalsRdd(rdd: RDD[(K,V)]): DStream[Boolean] = {
//      val rddReduced = rdd.reduceByKey(ring.add).cache
      dstream.transform[Boolean] { rdd1: RDD[(K,V)] =>
        //val reduced = r.reduceByKey(ring.add)
        rdd.reduceByKey(ring.add).fullOuterJoin(rdd1.reduceByKey(ring.add))
          .map { case (_,(r1Opt,r2Opt)) => ((),r1Opt.fold(false)(r1 => r2Opt.fold(false)(r2 => ring.equiv(r1,r2))))}
          .reduceByKey(_ && _).map(_._2)
      }
    }
  }

  implicit class DStreamImplicits[T](dstream: DStream[T]) {
    def profile(limit: Int = 10, toCache: List[RDD[_]])(implicit ssc: StreamingContext): Unit = {
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
          println(s"$counter - $totalTime ($interval) ($count)")
          time = currentTime
          if (counter == limit) done = true
        }
      }
      ssc.start()
      while(!done) { java.lang.Thread.sleep(100) }
      ssc.stop()
    }
  }

  def dstreamEqualsRdd[K:ClassTag,V:ClassTag](dstream: DStream[(K,V)], rdd: RDD[(K,V)])(implicit ring: Ring[V]): DStream[Boolean] = {
    val rddReduced = rdd.reduceByKey(ring.add).cache
    dstream.transform[Boolean] { rdd1: RDD[(K,V)] =>
      //val reduced = r.reduceByKey(ring.add)
      rddReduced.fullOuterJoin(rdd1.reduceByKey(ring.add))
        .map { case (_,(r1Opt,r2Opt)) => ((),r1Opt.fold(false)(r1 => r2Opt.fold(false)(r2 => ring.equiv(r1,r2))))}
        .reduceByKey(_ && _).map(_._2)
    }
  }

  def rddToDStream[T:ClassTag](rdd: RDD[T], n: Int = 5, rep: Int = 1)(implicit ssc: StreamingContext): DStream[T] = {
    val rddWithIds: RDD[(T,Long)] = rdd.zipWithUniqueId
    val queue = new mutable.Queue[RDD[T]]
    (0 until rep).foreach { _ =>
      (0 until n).map(i => rddWithIds.filter(_._2 % n == i)).foreach { r => queue.enqueue(r.map(_._1)) }
    }
    ssc.queueStream(queue, true)
  }

  //same as above but first batch contains 50%, rest is evenly split between the subsequent batches.
  def rddToDStream2[T:ClassTag](rdd: RDD[T], n: Int = 5)(implicit ssc: StreamingContext): DStream[T] = {
    val rddWithIds: RDD[(T,Long)] = rdd.zipWithUniqueId
    val queue = new mutable.Queue[RDD[T]]
    val firstBatch = rddWithIds.filter(_._2 % 2 == 0).map(_._1)
    queue.enqueue(firstBatch)
    val N = n - 1
    (0 until N).map(i => rddWithIds.filter(_._2 % (N*2) == (2*i)+1)).foreach { r => queue.enqueue(r.map(_._1)) }
    ssc.queueStream(queue, true)
  }

}


