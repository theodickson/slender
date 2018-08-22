/**
  * TODO
  * Can I encapsulate Boolean into Numeric by creating a custom typeclass?
  */
package slender

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import shapeless._
import shapeless.ops.hlist.Prepend

import scala.collection.immutable.Map
import scala.reflect.ClassTag


trait Ring[R] extends Serializable {
  def zero: R
  def add(x1: R, x2: R): R
  def not(x1: R): R
  def negate(x1: R): R
  def equiv(x1: R, x2: R): Boolean
}

/**Trait witnessing that C is a collection type with key of type K and value of type V*/
trait Collection[C,K,V]

/**Trait witnessing that R is not a collection type*/
trait NonCollection[R]

trait Multiply[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Dot[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Join[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Sum[T,S] extends (T => S) with Serializable

trait Group[T,S] extends (T => S) with Serializable

trait Mapper[F,T,S] extends ((T,F) => S) with Serializable

trait Deshard[T,S] extends (T => S) with Serializable

object Ring {

  implicit def BooleanRing: Ring[Boolean] = new Ring[Boolean] {
    def zero = false
    def one = true
    def add(t1: Boolean, t2: Boolean): Boolean = t1 || t2
    def not(t1: Boolean): Boolean = !t1
    def negate(t1: Boolean): Boolean = !t1
    def equiv(t1: Boolean, t2: Boolean) = t1 == t2
  }

  implicit def NumericRing[N](implicit num: Numeric[N]): Ring[N] = new Ring[N] {
    def zero: N = num.fromInt(0)
    def one: N = num.fromInt(1)
    def add(t1: N, t2: N): N = num.plus(t1,t2)
    def not(t1: N): N = if (t1 == zero) one else zero
    def negate(t1: N): N = num.negate(t1)
    def equiv(t1: N, t2: N): Boolean = num.equiv(t1, t2)
  }

  implicit def ProductRing[H,T <: HList](implicit rH: Ring[H], rT: Ring[T]): Ring[H :: T] =
    new Ring[H :: T] {
      def zero: H::T = rH.zero :: rT.zero
      def add(t1: H::T, t2: H::T): H::T = rH.add(t1.head, t2.head) :: rT.add(t1.tail,t2.tail)
      def not(t1: H::T): H::T = rH.not(t1.head) :: rT.not(t1.tail)
      def negate(t1: H::T): H::T = rH.negate(t1.head) :: rT.negate(t1.tail)
      def equiv(t1: H::T, t2: H::T): Boolean = rH.equiv(t1.head, t2.head) && rT.equiv(t1.tail, t2.tail)
    }

  implicit def RDDCollection[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Ring[RDD[(K,R)]] =
    new Ring[RDD[(K,R)]] {
      def zero = ??? //would need a context to produce an emptyRDD.
      def add(x1: RDD[(K,R)], x2: RDD[(K,R)]) =
        x1.union(x2)//.groupByKey.mapValues(_.reduce(ring.add))
      def not(x1: RDD[(K,R)]) = x1.mapValues(ring.not)
      def negate(x1: RDD[(K,R)]) = x1.mapValues(ring.negate)
      def equiv(x1: RDD[(K,R)], x2: RDD[(K,R)]): Boolean = {
        //aggregate both to be pure collections:
        val reduced1 = x1.reduceByKey(ring.add)
        val reduced2 = x2.reduceByKey(ring.add)
        //do a full outer join, so there is one row per unique key in either collection:
        reduced1.fullOuterJoin(reduced2)
          .map { case (_,(r1Opt,r2Opt)) =>
            //each key is equal in both collections if:
            //neither entry is None in the full outer join
            //and if neither is None, the contained values are equivalent.
            r1Opt.fold(false)(r1 => r2Opt.fold(false)(r2 => ring.equiv(r1,r2)))
          }.reduce(_ && _)
      }
//      def filter(c: RDD[(K,R)], f: (K,R) => Boolean): RDD[(K,R)] = c.filter { case (k, v) => f(k, v) }
    }

  //A single ring value distributed over an RDD
  implicit def RDDRing[R:ClassTag](implicit ring: Ring[R]): Ring[RDD[R]] = new Ring[RDD[R]] {
    def zero = ???
    def add(x1: RDD[R], x2: RDD[R]) = x1.union(x2)
    def not(x1: RDD[R]) = x1.map(ring.not)
    def negate(x1: RDD[R]) = x1.map(ring.negate)
    def equiv(x1: RDD[R], x2: RDD[R]): Boolean = ring.equiv(x1.reduce(ring.add), x1.reduce(ring.add))
  }

  //A single ring value distributed over a DStream
//  implicit def DStreamRing[R:ClassTag](implicit ring: Ring[R]): Ring[DStream[R]] = new Ring[DStream[R]] {
//    def zero = ???
//    def add(x1: DStream[R], x2: DStream[R]) = x1.union(x2)
//    def not(x1: DStream[R]) = x1.map(ring.not)
//    def negate(x1: DStream[R]) = x1.map(ring.negate)
//  }

//  implicit def DStreamRing[DS <: SlenderDStream[DS], R:ClassTag](implicit ring: Ring[R]): Ring[SlenderDStream.Aux[DS,R]] = new Ring[SlenderDStream[R]] {
//    def zero = ???
//    def add(x1: SlenderDStream[R], x2: SlenderDStream[R]) = ???
//    def not(x1: SlenderDStream[R]) = ???
//    def negate(x1: SlenderDStream[R]) = ???
//  }
//
////  implicit def DStreamCollection[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Ring[DStream[(K,R)]] =
////    new Ring[DStream[(K,R)]] {
////      def zero = ???
////      def add(x1: DStream[(K,R)], x2: DStream[(K,R)]) =
////        x1.union(x2)//.groupByKey.mapValues(_.reduce(ring.add))
////      def not(x1: DStream[(K,R)]) = x1.mapValues(ring.not)
////      def negate(x1: DStream[(K,R)]) = x1.mapValues(ring.negate)
////      //      def filter(c: RDD[(K,R)], f: (K,R) => Boolean): RDD[(K,R)] = c.filter { case (k, v) => f(k, v) }
////    }
//
//  implicit def DStreamCollection[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Ring[SlenderDStream[(K,R)]] =
//    new Ring[SlenderDStream[(K,R)]] {
//      def zero = ???
//      def add(x1: SlenderDStream[(K,R)], x2: SlenderDStream[(K,R)]) =
//        ???
//      def not(x1: SlenderDStream[(K,R)]) = ???
//      def negate(x1: SlenderDStream[(K,R)]) = ???
//    }

  implicit def MapRing[K, R](implicit ring: Ring[R]): Ring[Map[K,R]] =
    new Ring[Map[K,R]] {
      def zero: Map[K, R] = Map.empty[K,R]
      def add(t1: Map[K, R], t2: Map[K, R]): Map[K, R] =
        t1 ++ t2.map { case (k, v) => k -> ring.add(v, t1.getOrElse(k, ring.zero)) }
      def not(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.not(v)) }
      def negate(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.negate(v)) }
      def equiv(t1: Map[K,R], t2: Map[K,R]): Boolean = {
        //if they are the same size
        if (t1.size == t2.size) {
          //return whether all of the keys in t1 exist in t2 and have the same value there
          t1.forall { case (k,r1) =>
            t2.get(k).map(r2 => ring.equiv(r1, r2)).fold(false)(identity)
          }
        } else false
      }
//      def filter(c: Map[K, R], f: (K, R) => Boolean): Map[K, R] = c.filter { case (k, v) => f(k, v) }
    }
}

object Collection {
  /**RDDs and Maps are collections*/
  implicit def RddCollection[K,V]: Collection[RDD[(K,V)],K,V] = new Collection[RDD[(K,V)],K,V] {}
  implicit def DStreamCollection[DS <: SDStream[DS],K,V]: Collection[SDStream.Aux[DS,(K,V)],K,V] = new Collection[SDStream.Aux[DS,(K,V)],K,V] {}
//  implicit def DStreamCollection[DS <: SDStream[DS],K,V]: Collection[SDStream.Aux[DS,(K,V)],K,V] = new Collection[SDStream.Aux[DS,(K,V)],K,V] {}
//  implicit def IncDStreamCollection[K,V]: Collection[IncDStream.Aux[(K,V)],K,V] = new Collection[IncDStream.Aux[(K,V)],K,V] {}
//  implicit def AggDStreamCollection[K,V]: Collection[AggDStream.Aux[(K,V)],K,V] = new Collection[AggDStream.Aux[(K,V)],K,V] {}
  implicit def MapCollection[K,V]: Collection[Map[K,V],K,V] = new Collection[Map[K,V],K,V] {}
}

object NonCollection {
  /**Numerics and products are not collections*/
  implicit def BooleanNonCollection = new NonCollection[Boolean] {}
  implicit def NumericNonCollection[N:Numeric] = new NonCollection[N] {}
  implicit def ProductNonCollection[H <: HList] = new NonCollection[H] {}
}

object Multiply {

  def instance[T1,T2,O](f: (T1,T2) => O): Multiply[T1, T2, O] = new Multiply[T1,T2,O] {
    def apply(v1: T1, v2: T2): O = f(v1,v2)
  }

  implicit def BooleanMultiply: Multiply[Boolean,Boolean,Boolean] = instance { (t1,t2) => t1 && t2 }

  implicit def BooleanNumericMultiply[N](implicit num: Numeric[N]): Multiply[Boolean,N,N] = instance { (t1,t2) => if (t1) t2 else num.zero }

  implicit def NumericBooleanMultiply[N](implicit num: Numeric[N]): Multiply[N,Boolean,N] = instance { (t1,t2) => if (t2) t1 else num.zero }

  implicit def NumericMultiply[N](implicit num: Numeric[N]): Multiply[N,N,N] =
    instance { (t1,t2) => num.times(t1,t2) }

  implicit def mapMapMultiply[K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[Map[K, R1], Map[K, R2], Map[K, O]] =
    instance { (t1,t2) =>
      t1 map { case (k1, v1) =>
        k1 -> dot(v1, t2.getOrElse(k1, ring.zero))
      }
    }

  implicit def rddRddMultiply[K:ClassTag, R1:ClassTag, R2, O]
  (implicit dot: Dot[R1, R2, O]): Multiply[RDD[(K,R1)], RDD[(K,R2)], RDD[(K,O)]] =
    instance { (t1,t2) =>
      t1.join(t2) map { case (k, v) => k -> dot(v._1, v._2) }
    }

//  implicit def rddDStreamMultiply[K:ClassTag, R1:ClassTag, R2, O]
//  (implicit dot: Dot[R1, R2, O]): Multiply[RDD[(K,R1)], DStream[(K,R2)], DStream[(K,O)]] =
//    instance { (t1,t2) =>
//      //for each rdd in t2, join it to t1 and dot the pairs of values.
//      t2.transform { rdd => t1.join(rdd) map { case (k, v) => k -> dot(v._1, v._2) } }
//    }
  //allow an RDD to multiply with any DStream
  implicit def rddDStreamMultiply[DS <: SDStream[DS], K:ClassTag, R1:ClassTag, R2, O]
  (implicit dot: Dot[R1, R2, O]): Multiply[RDD[(K,R1)], SDStream.Aux[DS,(K,R2)], SDStream.Aux[DS,(K,O)]] =
    instance { (t1,t2) =>
      //for each rdd in t2, join it to t1 and dot the pairs of values.
      t2.map(_.transform { rdd => t1.join(rdd) map { case (k, v) => k -> dot(v._1, v._2) } })
    }


//  implicit def dStreamRDDMultiply[K:ClassTag, R1:ClassTag, R2:ClassTag, O]
//  (implicit dot: Dot[R1,R2,O]): Multiply[DStream[(K,R1)], RDD[(K,R2)], DStream[(K,O)]] =
//    instance { (t1,t2) =>
//      t1.transform { rdd => rdd.join(t2) map { case (k, v) => k -> dot(v._1, v._2) } }
//    }

  //allow any DStream to multiply with an RDD
  implicit def dStreamRDDMultiply[DS <: SDStream[DS], K:ClassTag, R1:ClassTag, R2:ClassTag, O]
  (implicit dot: Dot[R1,R2,O]): Multiply[SDStream.Aux[DS,(K,R1)], RDD[(K,R2)], SDStream.Aux[DS,(K,O)]] =
    instance { (t1,t2) =>
      t1.map(_.transform { rdd => rdd.join(t2) map { case (k, v) => k -> dot(v._1, v._2) } })
    }

  implicit def rddMapMultiply[K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[RDD[(K,R1)], Map[K, R2], RDD[(K,O)]] =
    instance { case (t1,t2) =>
      t1.map { case (k, v) =>
        k -> dot(v, t2.getOrElse(k, ring.zero))
      }
    }

  implicit def mapRddMultiply[K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], ring: Ring[R1]): Multiply[Map[K, R1], RDD[(K,R2)], RDD[(K,O)]] =
    instance { (t1,t2) =>
      t2.map { case (k, v) =>
        k -> dot(t1.getOrElse(k, ring.zero),v)
      }
    }

  implicit def infMultiply[C, K, R1, R2, O, C1, K1 >: K]
  (implicit coll: Collection[C,K,R1], dot: Dot[R1,R2,O], mapper: Mapper[(K,R1) => (K,O),C,C1]):
    Multiply[C, K1 => R2, C1] = instance { (t1,t2) =>
      mapper(t1,(k: K, r1: R1) => k -> dot(r1,t2(k)))
    }

}

object Dot {

  def instance[T1,T2,O](f: (T1,T2) => O): Dot[T1, T2, O] = new Dot[T1,T2,O] {
    def apply(v1: T1, v2: T2): O = f(v1,v2)
  }

  implicit def BooleanDot: Dot[Boolean,Boolean,Boolean] = instance { (t1,t2) => t1 && t2 }

  implicit def numericDot[N](implicit num: Numeric[N]): Dot[N, N, N] = instance { (t1,t2) => num.times(t1,t2) }

  implicit def BooleanNumericDot[N](implicit num: Numeric[N]): Dot[Boolean,N,N] = instance { (t1,t2) => if (t1) t2 else num.zero }

  implicit def NumericBooleanDot[N](implicit num: Numeric[N]): Dot[N,Boolean,N] = instance { (t1,t2) => if (t2) t1 else num.zero }

  implicit def mapMapDot[K1, K2, R1, R2, O]
  (implicit dot: Dot[R1, R2, O]): Dot[Map[K1, R1], Map[K2, R2], Map[(K1, K2), O]] =
    instance { (t1,t2) =>
      t1.flatMap { case (k1, v1) =>
        t2.map { case (k2, v2) => (k1, k2) -> dot(v1, v2) }
      }
    }

  implicit def rddRddDot[K1, K2, R1, R2, O](implicit dot: Dot[R1, R2, O]):
  Dot[RDD[(K1,R1)], RDD[(K2,R2)], RDD[((K1, K2),O)]] =
    instance { (t1,t2) =>
      t1.cartesian(t2).map { case ((k1, v1), (k2, v2)) => (k1, k2) -> dot(v1, v2) }
    }

  implicit def pushDownDotLeft[C,K,R,R1,O,C1]
  (implicit coll: Collection[C,K,R], dot: Dot[R,R1,O],nonColl: NonCollection[R1],
   mapper: Mapper[(K,R) => (K,O),C,C1]): Dot[C,R1,C1] =
    instance { (c,r1) => mapper(c,(k: K, r: R) => k -> dot(r,r1)) }

  implicit def pushDownDotRight[C,K,R,R1,O,C1]
  (implicit coll: Collection[C,K,R], dot: Dot[R1,R,O],nonColl: NonCollection[R1],
   mapper: Mapper[(K,R) => (K,O),C,C1]): Dot[R1,C,C1] =
    instance { (r1,c) => mapper(c,(k: K, r: R) => k -> dot(r1,r)) }

}

object Join {

  implicit def mapRddJoin[
  K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[Map[K::K1, R1], RDD[(K :: K2, R2)], RDD[(K :: K12, O)]] =
    new Join[Map[K::K1, R1], RDD[(K::K2, R2)], RDD[(K :: K12, O)]] {
      def apply(v1: Map[K::K1,R1], v2: RDD[(K :: K2, R2)]): RDD[(K :: K12, O)] = {
        val bmap = v1.map { case (k::k1,r1) => (k,(k1,r1)) }
        v2.flatMap { case (k::k2,r2) => bmap.get(k).map { case (k1,r1) => (k::prepend(k1,k2),dot(r1,r2)) } }
      }
    }

  implicit def rddMapJoin[
  K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[RDD[(K :: K1, R1)], Map[K::K2, R2], RDD[(K :: K12, O)]] =
    new Join[RDD[(K::K1, R1)], Map[K::K2, R2], RDD[(K :: K12, O)]] {
      def apply(v1: RDD[(K :: K1, R1)], v2: Map[K::K2,R2]): RDD[(K :: K12, O)] = {
        val bmap = v2.map { case (k::k2,r2) => (k,(k2,r2)) }
        v1.flatMap { case (k::k1,r1) => bmap.get(k).map { case (k2,r2) => (k::prepend(k1,k2),dot(r1,r2)) } }
      }
    }

  implicit def dstreamMapJoin[
  K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O, DS <: SDStream[DS]
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[SDStream.Aux[DS,(K :: K1, R1)], Map[K::K2, R2], SDStream.Aux[DS,(K :: K12, O)]] =
    new Join[SDStream.Aux[DS,(K::K1, R1)], Map[K::K2, R2], SDStream.Aux[DS,(K :: K12, O)]] {
      def apply(v1: SDStream.Aux[DS,(K :: K1, R1)], v2: Map[K::K2,R2]): SDStream.Aux[DS,(K :: K12, O)] = {
        val bmap = v2.map { case (k::k2,r2) => (k,(k2,r2)) }
        v1.map(_.transform(_.flatMap { case (k::k1,r1) => bmap.get(k).map { case (k2,r2) => (k::prepend(k1,k2),dot(r1,r2)) } }))
      }
    }

  implicit def rddRddJoin[
    K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[RDD[(K :: K1, R1)], RDD[(K :: K2, R2)], RDD[(K :: K12, O)]] =
    new Join[RDD[(K :: K1, R1)], RDD[(K :: K2, R2)], RDD[(K :: K12, O)]] {
      def apply(v1: RDD[(K :: K1, R1)], v2: RDD[(K :: K2, R2)]): RDD[(K :: K12, O)] = {
        val left = v1.map { case (k :: k1, r1) => (k, (k1, r1)) }
        val right = v2.map { case (k :: k2, r2) => (k, (k2, r2)) }
        left.join(right).map { case (k, ((k1, r1), (k2, r2))) => ((k :: (prepend(k1, k2))), dot(r1, r2)) }
      }
    }

  implicit def rddDStreamJoin[
  DS <: SDStream[DS], K:ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[RDD[(K :: K1, R1)], SDStream.Aux[DS,(K :: K2, R2)], SDStream.Aux[DS,(K :: K12, O)]] =
    new Join[RDD[(K :: K1, R1)], SDStream.Aux[DS,(K :: K2, R2)], SDStream.Aux[DS,(K :: K12, O)]] {
      def apply(v1: RDD[(K :: K1, R1)], v2: SDStream.Aux[DS,(K :: K2, R2)]): SDStream.Aux[DS,(K :: K12, O)] = {
        val left = v1.map { case (k :: k1, r1) => (k, (k1, r1)) }
        val right = v2.map(_.map { case (k :: k2, r2) => (k, (k2, r2)) })
        right.map(_.transform { rdd =>
          left.join(rdd).map { case (k, ((k1, r1), (k2, r2))) => ((k :: (prepend(k1, k2))), dot(r1, r2)) }
        })
      }
    }

//  //doesnt broadcast agg dstreams
//  implicit def dstreamRDDJoin[
//    K:ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
//  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[AggDStream.Aux[(K :: K1, R1)], RDD[(K :: K2, R2)], AggDStream.Aux[(K :: K12, O)]] =
//    new Join[AggDStream.Aux[(K :: K1, R1)], RDD[(K :: K2, R2)], AggDStream.Aux[(K :: K12, O)]] {
//      def apply(v1: AggDStream.Aux[(K :: K1, R1)], v2: RDD[(K :: K2, R2)]): AggDStream.Aux[(K :: K12, O)] = {
//        val left = v1.map(_.map { case (k :: k1, r1) => (k, (k1, r1)) })
//        val right = v2.map { case (k :: k2, r2) => (k, (k2, r2)) }
//        left.map(_.transform { rdd =>
//          rdd.join(right).map { case (k, ((k1, r1), (k2, r2))) => ((k :: (prepend(k1, k2))), dot(r1, r2)) }
//        })
//      }
//    }
//
//  //only broadcasts inc dstreams
//  implicit def dstreamRDDJoinBC[
//    K:ClassTag, K1 <: HList, K2 <: HList, R1:ClassTag, R2, O:ClassTag
//  ](implicit join: Join[Map[K::K1,R1],RDD[(K::K2,R2)],RDD[O]]): Join[IncDStream.Aux[(K :: K1, R1)], RDD[(K :: K2, R2)], IncDStream.Aux[O]] =
//    new Join[IncDStream.Aux[(K :: K1, R1)], RDD[(K :: K2, R2)], IncDStream.Aux[O]] {
//      def apply(v1: IncDStream.Aux[(K :: K1, R1)], v2: RDD[(K :: K2, R2)]): IncDStream.Aux[O] = {
//        v1.map(_.transform { rdd =>
//          val asMap: Map[K::K1,R1] = rdd.collectAsMap.toMap
//          join(asMap, v2)
//        })
//      }
//    }

  //blanket broadcaster
  implicit def dstreamRDDJoinBC[
  DS <: SDStream[DS],K:ClassTag, K1 <: HList, K2 <: HList, R1:ClassTag, R2, O:ClassTag
  ](implicit join: Join[Map[K::K1,R1],RDD[(K::K2,R2)],RDD[O]]): Join[SDStream.Aux[DS,(K :: K1, R1)], RDD[(K :: K2, R2)], SDStream.Aux[DS,O]] =
    new Join[SDStream.Aux[DS,(K :: K1, R1)], RDD[(K :: K2, R2)], SDStream.Aux[DS,O]] {
      def apply(v1: SDStream.Aux[DS,(K :: K1, R1)], v2: RDD[(K :: K2, R2)]): SDStream.Aux[DS,O] = {
        v1.map(_.transform { rdd =>
          val asMap: Map[K::K1,R1] = rdd.collectAsMap.toMap
          join(asMap, v2)
        })
      }
    }
}

object Sum {

  def instance[T,O](f: T => O): Sum[T,O] = new Sum[T,O] {
    def apply(v1: T): O = f(v1)
  }

  implicit def MapSum[K, R](implicit ring: Ring[R]): Sum[Map[K,R],R] =
    instance { _.values.reduce(ring.add) }

  implicit def RddNumericSum[K :ClassTag, N :ClassTag](implicit ring: Ring[N], numeric: Numeric[N]): Sum[RDD[(K,N)],RDD[N]] =
    instance { _.values } //.reduce(ring.add)

  implicit def RddBooleanSum[K :ClassTag](implicit ring: Ring[Boolean]): Sum[RDD[(K,Boolean)],RDD[Boolean]] =
    instance { _.values } //.reduce(ring.add)

  implicit def RddMapSum[K :ClassTag, K1 :ClassTag, R1 :ClassTag]
  (implicit ring: Ring[R1], tag:ClassTag[Map[K1,R1]]): Sum[RDD[(K,Map[K1,R1])],RDD[(K1,R1)]] =
    instance { v1 =>
      v1.values.flatMap(x => x)//.reduceByKey(ring.add)
    }

//  implicit def DStreamNumericSum[K :ClassTag, N :ClassTag](implicit ring: Ring[N], numeric: Numeric[N]): Sum[DStream[(K,N)],DStream[N]] =
//    instance { _.map(_._2) } //.reduce(ring.add)
//
//  implicit def DStreamBooleanSum[K :ClassTag](implicit ring: Ring[Boolean]): Sum[DStream[(K,Boolean)],DStream[Boolean]] =
//    instance { _.map(_._2) } //.reduce(ring.add)
//
//  implicit def DStreamMapSum[K :ClassTag, K1 :ClassTag, R1 :ClassTag]
//  (implicit ring: Ring[R1], tag:ClassTag[Map[K1,R1]]): Sum[DStream[(K,Map[K1,R1])],DStream[(K1,R1)]] =
//    instance { v1 =>
//      v1.map(_._2).flatMap(x => x)//.reduceByKey(ring.add)
//    }

  implicit def DStreamNumericSum[DS <: SDStream[DS], K:ClassTag, N:ClassTag]
  (implicit ring: Ring[N], numeric: Numeric[N]): Sum[SDStream.Aux[DS,(K,N)],SDStream.Aux[DS,N]] =
    instance { _.map(_.map(_._2)) } //.reduce(ring.add)

  implicit def DStreamBooleanSum[DS <: SDStream[DS],K:ClassTag](implicit ring: Ring[Boolean]): Sum[SDStream.Aux[DS,(K,Boolean)],SDStream.Aux[DS,Boolean]] =
    instance { _.map(_.map(_._2)) } //.reduce(ring.add)

  implicit def DStreamMapSum[DS <: SDStream[DS], K:ClassTag, K1:ClassTag, R1:ClassTag]
  (implicit ring: Ring[R1], tag:ClassTag[Map[K1,R1]]): Sum[SDStream.Aux[DS,(K,Map[K1,R1])],SDStream.Aux[DS,(K1,R1)]] =
    instance { v1 =>
      v1.map(_.map(_._2).flatMap(x => x))//.reduceByKey(ring.add)
    }
}


object Group {

  //Note - when this was done generically, it meant that when grouping pairs, the grouped 'second' element was always
  //treated as an HList even when it was just e.g. K2::HNil, which meant getting a map result type with a product of
  //just one element. And, even with scoping, the special case of just K1::K2::HNil was not being used ahead of the
  //generic case. Hence for now just have two special cases, one for pairs and one for triples.

  //todo - might be able to unwrap single-element products in deeptupler and so go back to the generic method here.
  implicit def RddGroup2[K1:ClassTag,K2:ClassTag,R](implicit ring: Ring[R]): Group[RDD[(K1::K2::HNil,R)],RDD[(K1::Map[K2,R]::HNil,Boolean)]] =
    new Group[RDD[(K1::K2::HNil,R)],RDD[(K1::Map[K2,R]::HNil,Boolean)]] {

      def iterableToMap(x: Iterable[(K2,R)]): Map[K2,R] = x.foldRight(Map.empty[K2,R]) {
        case ((k2,r),mp) => {
          val prevValue = mp.getOrElse[R](k2,ring.zero)
          val newValue = ring.add(prevValue,r)
          mp + ((k2,newValue))
        }
      }

      def apply(v1: RDD[(K1::K2::HNil,R)]): RDD[(K1::Map[K2,R]::HNil,Boolean)] = {
        val grouped = v1.groupBy(_._1.head).mapValues(_.map { case (_::k2::HNil,v) => (k2,v) }).mapValues(iterableToMap)
        grouped.map[(K1::Map[K2,R]::HNil,Boolean)] { case (k1,k2s) => (k1::k2s::HNil,true) }
      }
    }

  implicit def RddGroup3[K1:ClassTag,K2:ClassTag,K3:ClassTag,R]
  (implicit ring: Ring[R]): Group[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]] =
    new Group[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]] {

      def iterableToMap(x: Iterable[(K2::K3::HNil,R)]): Map[K2::K3::HNil,R] = x.foldRight(Map.empty[K2::K3::HNil,R]) {
        case ((k2k3,r),mp) => {
          val prevValue = mp.getOrElse[R](k2k3,ring.zero)
          val newValue = ring.add(prevValue,r)
          mp + ((k2k3,newValue))
        }
      }

      def apply(v1: RDD[(K1::K2::K3::HNil,R)]): RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)] = {
        val grouped = v1.groupBy(_._1.head).mapValues(_.map { case (_::k2::k3::HNil,v) => (k2::k3::HNil,v) }).mapValues(iterableToMap)
        grouped.map[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)] { case (k1,k2s) => (k1::k2s::HNil,true) }
      }
    }

  //In order to group a dstream, we must produce the cumulative version first and group this.
  implicit def DStreamGroup2[DS <: SDStream[DS],K1:ClassTag,K2:ClassTag,R]
  (implicit ring: Ring[R], acc: Accumulate[DS,(K1::K2::HNil,R)]):
    Group[SDStream.Aux[DS,(K1::K2::HNil,R)],AggDStream.Aux[(K1::Map[K2,R]::HNil,Boolean)]] =
    new Group[SDStream.Aux[DS,(K1::K2::HNil,R)],AggDStream.Aux[(K1::Map[K2,R]::HNil,Boolean)]] {

      def iterableToMap(x: Iterable[(K2,R)]): Map[K2,R] = x.foldRight(Map.empty[K2,R]) {
        case ((k2,r),mp) => {
          val prevValue = mp.getOrElse[R](k2,ring.zero)
          val newValue = ring.add(prevValue,r)
          mp + ((k2,newValue))
        }
      }

      def apply(v1: SDStream.Aux[DS,(K1::K2::HNil,R)]): AggDStream.Aux[(K1::Map[K2,R]::HNil,Boolean)] = {
        val accumulated = acc(v1) //first get the cumulative version of the input (this may be no change if the input is already cumulative)
        accumulated.map { ds =>
          val grouped = ds.transform(_.groupBy(_._1.head).mapValues(_.map { case (_::k2::HNil,v) => (k2,v) }).mapValues(iterableToMap))
          grouped.map[(K1::Map[K2,R]::HNil,Boolean)] { case (k1,k2s) => (k1::k2s::HNil,true) }
        }
      }
    }

  //In order to group a dstream, we must produce the cumulative version first and group this.
  implicit def DStreamGroup3[DS <: SDStream[DS],K1:ClassTag,K2:ClassTag,K3:ClassTag,R]
  (implicit ring: Ring[R], acc: Accumulate[DS,(K1::K2::K3::HNil,R)]):
  Group[SDStream.Aux[DS,(K1::K2::K3::HNil,R)],AggDStream.Aux[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]] =
    new Group[SDStream.Aux[DS,(K1::K2::K3::HNil,R)],AggDStream.Aux[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]] {

      def iterableToMap(x: Iterable[(K2::K3::HNil,R)]): Map[K2::K3::HNil,R] = x.foldRight(Map.empty[K2::K3::HNil,R]) {
        case ((k2k3,r),mp) => {
          val prevValue = mp.getOrElse[R](k2k3,ring.zero)
          val newValue = ring.add(prevValue,r)
          mp + ((k2k3,newValue))
        }
      }

      def apply(v1: SDStream.Aux[DS,(K1::K2::K3::HNil,R)]): AggDStream.Aux[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)] = {
        val accumulated = acc(v1) //first get the cumulative version of the input (this may be no change if the input is already cumulative)
        accumulated.map { ds =>
          val grouped = ds.transform(_.groupBy(_._1.head).mapValues(_.map { case (_::k2::k3::HNil,v) => (k2::k3::HNil,v) }).mapValues(iterableToMap))
          grouped.map[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)] { case (k1,k2s) => (k1::k2s::HNil,true) }
        }
      }
    }

}



object Mapper {

  implicit def mapMapper[K,R,R1]: Mapper[(K,R) => (K,R1),Map[K,R],Map[K,R1]] = new Mapper[(K,R) => (K,R1),Map[K,R],Map[K,R1]] {
    def apply(v1: Map[K,R], v2: (K,R) => (K,R1)): Map[K,R1] = v1.map { case (k,v) => v2(k,v) }
  }

  implicit def rddMapper[K,R,R1]: Mapper[(K,R) => (K,R1),RDD[(K,R)],RDD[(K,R1)]] = new Mapper[(K,R) => (K,R1),RDD[(K,R)],RDD[(K,R1)]] {
    def apply(v1: RDD[(K,R)], v2: (K,R) => (K,R1)): RDD[(K,R1)] = v1.map { case (k,v) => v2(k,v) }
  }

//  implicit def dstreamMapper[K,R,R1]: Mapper[(K,R) => (K,R1),DStream[(K,R)],DStream[(K,R1)]] = new Mapper[(K,R) => (K,R1),DStream[(K,R)],DStream[(K,R1)]] {
//    def apply(v1: DStream[(K,R)], v2: (K,R) => (K,R1)): DStream[(K,R1)] = v1.map { case (k,v) => v2(k,v) }
//  }

  implicit def dstreamMapper[DS <: SDStream[DS],K,R,R1]: Mapper[(K,R) => (K,R1),SDStream.Aux[DS,(K,R)],SDStream.Aux[DS,(K,R1)]] = new Mapper[(K,R) => (K,R1),SDStream.Aux[DS,(K,R)],SDStream.Aux[DS,(K,R1)]] {
    def apply(v1: SDStream.Aux[DS,(K,R)], v2: (K,R) => (K,R1)): SDStream.Aux[DS,(K,R1)] = v1.map(_.map { case (k,v) => v2(k,v) })
  }

}

object Deshard {

  implicit def RddDeshard[K:ClassTag,R:ClassTag](implicit ring: Ring[R]): Deshard[RDD[(K,R)],RDD[(K,R)]] =
    new Deshard[RDD[(K,R)],RDD[(K,R)]] {
      def apply(v1: RDD[(K,R)]): RDD[(K,R)] = v1.reduceByKey(ring.add)
    }
}