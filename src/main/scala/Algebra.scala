/**
  * TODO
  * Group implementation is incorrect
  * Mapper is ugly
  * Cannot do ops on numerics of different types which effectively rules out anything besides Ints.
  * Want to figure out why Aux pattern is not working with Collection
  * Do I need non-collection? (think so)
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

//todo - subsume into Collection again?
trait Mapper[F,T,S] extends ((T,F) => S) with Serializable

object Ring {

  implicit def BooleanRing: Ring[Boolean] = new Ring[Boolean] {
    def zero = false
    def one = true
    def add(t1: Boolean, t2: Boolean): Boolean = t1 || t2
    def not(t1: Boolean): Boolean = !t1
    def negate(t1: Boolean): Boolean = !t1
  }

  implicit def NumericRing[N](implicit num: Numeric[N]): Ring[N] = new Ring[N] {
    def zero: N = num.fromInt(0)
    def one: N = num.fromInt(1)
    def add(t1: N, t2: N): N = num.plus(t1,t2)
    def not(t1: N): N = if (t1 == zero) one else zero
    def negate(t1: N): N = num.negate(t1)
  }

  implicit def ProductRing[H,T <: HList](implicit rH: Ring[H], rT: Ring[T]): Ring[H :: T] =
    new Ring[H :: T] {
      def zero: H::T = rH.zero :: rT.zero
      def add(t1: H::T, t2: H::T): H::T = rH.add(t1.head, t2.head) :: rT.add(t1.tail,t2.tail)
      def not(t1: H::T): H::T = rH.not(t1.head) :: rT.not(t1.tail)
      def negate(t1: H::T): H::T = rH.negate(t1.head) :: rT.negate(t1.tail)
    }

  implicit def RDDRing[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Ring[RDD[(K,R)]] =
    new Ring[RDD[(K,R)]] {
      def zero = ??? // todo - need Spark context do initialize empty RDD but then cant serialize. however shouldnt ever need emptyRDD in practice.
      def add(x1: RDD[(K,R)], x2: RDD[(K,R)]) =
        x1.union(x2).groupByKey.mapValues(_.reduce(ring.add))
      def not(x1: RDD[(K,R)]) = x1.mapValues(ring.negate)
      def negate(x1: RDD[(K,R)]) = x1.mapValues(ring.negate)
//      def filter(c: RDD[(K,R)], f: (K,R) => Boolean): RDD[(K,R)] = c.filter { case (k, v) => f(k, v) }
    }

  implicit def DStreamRing[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Ring[DStream[(K,R)]] =
    new Ring[DStream[(K,R)]] {
      def zero = ??? // todo - need Spark context do initialize empty DStream but then cant serialize. however shouldnt ever need emptyDStream in practice.
      def add(x1: DStream[(K,R)], x2: DStream[(K,R)]) =
        x1.union(x2).groupByKey.mapValues(_.reduce(ring.add)) //todo not necessary if consider sharded
      def not(x1: DStream[(K,R)]) = ???  //x1.mapValues(ring.negate) doesnt work across multiple RDDs..
      def negate(x1: DStream[(K,R)]) = x1.mapValues(ring.negate)
      //      def filter(c: RDD[(K,R)], f: (K,R) => Boolean): RDD[(K,R)] = c.filter { case (k, v) => f(k, v) }
    }

  implicit def MapRing[K, R](implicit ring: Ring[R]): Ring[Map[K,R]] =
    new Ring[Map[K,R]] {
      def zero: Map[K, R] = Map.empty[K,R]
      def add(t1: Map[K, R], t2: Map[K, R]): Map[K, R] =
        t1 ++ t2.map { case (k, v) => k -> ring.add(v, t1.getOrElse(k, ring.zero)) }
      def not(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.not(v)) }
      def negate(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.negate(v)) }
//      def filter(c: Map[K, R], f: (K, R) => Boolean): Map[K, R] = c.filter { case (k, v) => f(k, v) }
    }
}

object Collection {
  /**RDDs and Maps are collections*/
  implicit def RddCollection[K,V]: Collection[RDD[(K,V)],K,V] = new Collection[RDD[(K,V)],K,V] {}
  implicit def DStreamCollection[K,V]: Collection[DStream[(K,V)],K,V] = new Collection[DStream[(K,V)],K,V] {}
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

  implicit def rddDStreamMultiply[K:ClassTag, R1:ClassTag, R2, O]
  (implicit dot: Dot[R1, R2, O]): Multiply[RDD[(K,R1)], DStream[(K,R2)], DStream[(K,O)]] =
    instance { (t1,t2) =>
      //for each rdd in t2, join it to t1 and dot the pairs of values.
      t2.transform { rdd => t1.join(rdd) map { case (k, v) => k -> dot(v._1, v._2) } }
    }

  implicit def dStreamRDDMultiply[K:ClassTag, R1:ClassTag, R2:ClassTag, O]
  (implicit dot: Dot[R1,R2,O]): Multiply[DStream[(K,R1)], RDD[(K,R2)], DStream[(K,O)]] =
    instance { (t1,t2) =>
      t1.transform { rdd => rdd.join(t2) map { case (k, v) => k -> dot(v._1, v._2) } }
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
  K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[RDD[(K :: K1, R1)], DStream[(K :: K2, R2)], DStream[(K :: K12, O)]] =
    new Join[RDD[(K :: K1, R1)], DStream[(K :: K2, R2)], DStream[(K :: K12, O)]] {
      def apply(v1: RDD[(K :: K1, R1)], v2: DStream[(K :: K2, R2)]): DStream[(K :: K12, O)] = {
        val left = v1.map { case (k :: k1, r1) => (k, (k1, r1)) }
        val right = v2.map { case (k :: k2, r2) => (k, (k2, r2)) }
        right.transform { rdd =>
          left.join(rdd).map { case (k, ((k1, r1), (k2, r2))) => ((k :: (prepend(k1, k2))), dot(r1, r2)) }
        }
      }
    }

  implicit def dstreamRDDJoin[
  K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[DStream[(K :: K1, R1)], RDD[(K :: K2, R2)], DStream[(K :: K12, O)]] =
    new Join[DStream[(K :: K1, R1)], RDD[(K :: K2, R2)], DStream[(K :: K12, O)]] {
      def apply(v1: DStream[(K :: K1, R1)], v2: RDD[(K :: K2, R2)]): DStream[(K :: K12, O)] = {
        val left = v1.map { case (k :: k1, r1) => (k, (k1, r1)) }
        val right = v2.map { case (k :: k2, r2) => (k, (k2, r2)) }
        left.transform { rdd =>
          rdd.join(right).map { case (k, ((k1, r1), (k2, r2))) => ((k :: (prepend(k1, k2))), dot(r1, r2)) }
        }
      }
    }
//
//  implicit def dstreamDstreamJoin[
//  K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
//  ](implicit ring1: Ring[R1], ring2: Ring[R2], dot: Dot[R1, R2, O], prepend: Prepend.Aux[K1, K2, K12]): Join[DStream[(K :: K1, R1)], DStream[(K :: K2, R2)], DStream[(K :: K12, O)]] =
//    new Join[DStream[(K :: K1, R1)], DStream[(K :: K2, R2)], DStream[(K :: K12, O)]] {
//      def apply(v1: DStream[(K :: K1, R1)], v2: DStream[(K :: K2, R2)]): DStream[(K :: K12, O)] = {
//        //X
//        val left = v1.map { case (k :: k1, r1) => (k, (k1, r1)) }
//        //Y
//        val right = v2.map { case (k :: k2, r2) => (k, (k2, r2)) }
//        //Xc'
//        val leftCumulative = left.updateStateByKey[R1]((pairs,acc) =>
//          Some(
//            ring1.add(acc.getOrElse(ring1.zero), pairs.map(_._2).reduce(ring1.add))
//          )
//        )
//        //Yc'
//        val rightCumulative = right.updateStateByKey[R2]((pairs,acc) =>
//          Some(
//            ring2.add(acc.getOrElse(ring2.zero), pairs.map(_._2).reduce(ring2.add))
//          )
//        )
//        //Xc
//        val leftC = leftCumulative //todo diff
//        val rightC = rightCumulative //todo diff
//        //todo implicitly use RDDJoins etc... wont need to do the maps above either on v1 or v2.
//        val justDeltas = leftC.transformWith(rightC, (l,r) => l.)
//        left.transform { rdd =>
//          rdd.join(right).map { case (k, ((k1, r1), (k2, r2))) => ((k :: (prepend(k1, k2))), dot(r1, r2)) }
//        }
//      }
//    }
}

object Sum {

  def instance[T,O](f: T => O): Sum[T,O] = new Sum[T,O] {
    def apply(v1: T): O = f(v1)
  }

  implicit def MapSum[K, R](implicit ring: Ring[R]): Sum[Map[K,R],R] =
    instance { _.values.reduce(ring.add) }

  implicit def RddNumericSum[K :ClassTag, N :ClassTag](implicit ring: Ring[N], numeric: Numeric[N]): Sum[RDD[(K,N)],N] =
    instance { _.values.reduce(ring.add) }

  implicit def RddBooleanSum[K :ClassTag](implicit ring: Ring[Boolean]): Sum[RDD[(K,Boolean)],Boolean] =
    instance { _.values.reduce(ring.add) }

  implicit def RddMapSum[K :ClassTag, K1 :ClassTag, R1 :ClassTag]
  (implicit ring: Ring[R1], tag:ClassTag[Map[K1,R1]]): Sum[RDD[(K,Map[K1,R1])],RDD[(K1,R1)]] =
    instance { v1 =>
      v1.values.flatMap(x => x).reduceByKey(ring.add)
    }

  //todo - dstreamnumericsum

  implicit def DStreamMapSum[K :ClassTag, K1 :ClassTag, R1 :ClassTag]
  (implicit ring: Ring[R1], tag:ClassTag[Map[K1,R1]]): Sum[DStream[(K,Map[K1,R1])],DStream[(K1,R1)]] =
    instance { v1 =>
      v1.map(_._2).flatMap(x => x).reduceByKey(ring.add)
    }
}


object Group {

  //Note - when this was done generically, it meant that when grouping pairs, the grouped 'second' element was always
  //treated as an HList even when it was just e.g. K2::HNil, which meant getting a map result type with a product of
  //just one element. And, even with scoping, the special case of just K1::K2::HNil was not being used ahead of the
  //generic case. Hence for now just have two special cases, one for pairs and one for triples.
  implicit def RddGroup2[K1:ClassTag,K2:ClassTag,R](implicit ring: Ring[R]): Group[RDD[(K1::K2::HNil,R)],RDD[(K1::Map[K2,R]::HNil,Boolean)]] =
    new Group[RDD[(K1::K2::HNil,R)],RDD[(K1::Map[K2,R]::HNil,Boolean)]] {
      def apply(v1: RDD[(K1::K2::HNil,R)]): RDD[(K1::Map[K2,R]::HNil,Boolean)] = {
        val grouped = v1.groupBy(_._1.head) //RDD[(K1,Iterable[(K1::K2,R)])]
        //we need to turn the iterable of (K1::K2,R) pairs into a Map[K2,R]
        //since we have grouped by K1, we know that the K2s will be unique in each iterable, hence can just map to (k2,v)
        //pairs and use toMap. (normally toMap would be dangerous here since it overwrites)
        val grouped = v1.groupBy(_._1.head).mapValues[Map[K2,R]] { _.map { case (_::k2::HNil,v) => (k2,v) } toMap }
        grouped.map[(K1::Map[K2,R]::HNil,Boolean)] { case (k1,k2s) => (k1::k2s::HNil,true) }
      }
    }

  implicit def RddGroup3[K1:ClassTag,K2:ClassTag,K3:ClassTag,R]
  (implicit ring: Ring[R]): Group[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]] =
    new Group[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]] {
      def apply(v1: RDD[(K1::K2::K3::HNil,R)]): RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)] = {
        val grouped = v1.groupBy(_._1.head).mapValues[Map[K2::K3::HNil,R]] { _.map { case (_::k2::k3::HNil,v) => (k2::k3::HNil,v) } toMap }
        grouped.map[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)] { case (k1,k2s) => (k1::k2s::HNil,true) }
      }
    }

  //todo - DStreamGroup - obviously not possible to do efficiently but is there some sort of hack that can be done here?
  //to make testing against the recomputed version more elegant?

  //todo - map group
}



object Mapper {

  implicit def mapMapper[K,R,R1]: Mapper[(K,R) => (K,R1),Map[K,R],Map[K,R1]] = new Mapper[(K,R) => (K,R1),Map[K,R],Map[K,R1]] {
    def apply(v1: Map[K,R], v2: (K,R) => (K,R1)): Map[K,R1] = v1.map { case (k,v) => v2(k,v) }
  }

  implicit def rddMapper[K,R,R1]: Mapper[(K,R) => (K,R1),RDD[(K,R)],RDD[(K,R1)]] = new Mapper[(K,R) => (K,R1),RDD[(K,R)],RDD[(K,R1)]] {
    def apply(v1: RDD[(K,R)], v2: (K,R) => (K,R1)): RDD[(K,R1)] = v1.map { case (k,v) => v2(k,v) }
  }

  implicit def dstreamMapper[K,R,R1]: Mapper[(K,R) => (K,R1),DStream[(K,R)],DStream[(K,R1)]] = new Mapper[(K,R) => (K,R1),DStream[(K,R)],DStream[(K,R1)]] {
    def apply(v1: DStream[(K,R)], v2: (K,R) => (K,R1)): DStream[(K,R1)] = v1.map { case (k,v) => v2(k,v) }
  }
}