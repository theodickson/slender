import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait types {
  type BoundVars = Map[String,Any]
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

  implicit class AnyImplicits[T](x: T) {

    def printType(implicit tag: WeakTypeTag[T]): Unit = println(prettyType(tag))

  }

  implicit class RddTestImplicits[T](rdd: RDD[T]) {

    //    implicit def unsafeMapOrdering[K,V]: Ordering[Map[K,V]] = new Ordering[Map[K,V]] {
    //      def compare(x: Map[K,V], y: Map[K,V]): Int = ???
    //    }
    def dataToString: String = rdd.take(10).map(_.toString).mkString("\n")
    def printData: Unit = print(dataToString)
    //    def ~=(other: RDD[(K,V)])(implicit ord: Ordering[K]): Boolean = {
    //      val c1 = rdd.count; val c2 = other.count
    //      if (c1 == c2) {
    //        rdd.sortByKey(ascending=true).zip(other.sortByKey(ascending=true)).map { case (x,y) => x == y } reduce (_ && _)
    //      } else false
    //    }
  }

  def rddToDStream[T:ClassTag](rdd: RDD[T], n: Int = 5, rep: Int = 1)(implicit ssc: StreamingContext): DStream[T] = {
    val rddWithIds: RDD[(T,Long)] = rdd.zipWithUniqueId
    val queue = new mutable.Queue[RDD[T]]
    (0 until rep).foreach { _ =>
      (0 until n).map(i => rddWithIds.filter(_._2 % n == i)).foreach { r => queue.enqueue(r.map(_._1)) }
    }
    ssc.queueStream(queue,true)
  }

//  case class Counter() extends Serializable {
//    var i = 0
//    def inc: Unit = i += 1
//  }



}


