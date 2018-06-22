package slender

import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.immutable.Map
import scala.reflect.ClassTag
import shapeless._
import shapeless.ops.hlist.Prepend

trait Ring[R] extends Serializable {
  def zero: R
  def add(x1: R, x2: R): R
  def not(x1: R): R
  def negate(x1: R): R
  //def filterZeros(x1: R): R = x1
}

trait Collection2[C,K,R] extends Ring[C] {
  def innerRing: Ring[R]
  def mapValues[R1,C1](f: R => R1, c: C): C1 = {
    val mapper = implicitly[Mapper[R,R1,C,C1]]
    mapper(f,c)
  }
}

trait Collection[C[_,_],K,R] extends Ring[C[K,R]] {
  def innerRing: Ring[R]
  def map[R1](c: C[K,R], f: (K,R) => (K,R1)): C[K,R1]
//  def filter(c: C[K,R], f: (K,R) => Boolean): C[K,R]
//  override def filterZeros(c: C[K,R]): C[K,R] = {
//    val recursivelyFiltered = map(c, (k,r) => (k,innerRing.filterZeros(r)))
//    filter(recursivelyFiltered, (k,r) => r != innerRing.zero)
//  }
}

trait NonCollectionRing[R] extends Ring[R]

trait NumericRing[R] extends NonCollectionRing[R] {
  def num: Numeric[R]
  def zero: R = num.fromInt(0)
  def one: R = num.fromInt(1)
  def add(t1: R, t2: R): R = num.plus(t1,t2)
  def not(t1: R): R = if (t1 == zero) one else zero
  def negate(t1: R): R = num.negate(t1)
}

trait Multiply[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Dot[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Join[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Sum[T,S] extends (T => S) with Serializable

trait Group[T,S] extends (T => S) with Serializable

trait Mapper[R,R1,C,C1] extends (((R => R1),C) => C1) with Serializable

object Mapper {
  implicit def mapMapper[K,R,R1]: Mapper[R,R1,Map[K,R],Map[K,R1]] = new Mapper[R,R1,Map[K,R],Map[K,R1]] {
    def apply(v1: R => R1, v2: Map[K,R]): Map[K,R1] = v2.mapValues(v1)
  }

  implicit def rddMapper[K,R,R1]: Mapper[R,R1,RDD[(K,R)],RDD[(K,R1)]] = new Mapper[R,R1,RDD[(K,R)],RDD[(K,R1)]] {
    def apply(v1: R => R1, v2: RDD[(K,R)]): RDD[(K,R1)] = v2.mapValues(v1)
  }
}

object Ring {

  implicit def NumericRing[N](implicit ev: Numeric[N]): NumericRing[N] = new NumericRing[N] { val num = ev }

  implicit def ProductRing[H,T <: HList](implicit rH: Ring[H], rT: Ring[T]): NonCollectionRing[H :: T] =
    new NonCollectionRing[H :: T] {
      def zero: H::T = rH.zero :: rT.zero
      def add(t1: H::T, t2: H::T): H::T = rH.add(t1.head, t2.head) :: rT.add(t1.tail,t2.tail)
      def not(t1: H::T): H::T = rH.not(t1.head) :: rT.not(t1.tail)
      def negate(t1: H::T): H::T = rH.negate(t1.head) :: rT.negate(t1.tail)
    }

//  implicit def RDDCollection[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Collection[PairRDD, K, R] =
//    new Collection[PairRDD, K, R] {
//      val innerRing = ring
//      def zero = ??? // todo - need Spark context do initialize empty RDD but then cant serialize. however shouldnt ever need emptyRDD in practice.
//      def add(x1: PairRDD[K, R], x2: PairRDD[K, R]) =
//        x1.union(x2.rdd).groupByKey.mapValues(_.reduce(ring.add))
//      def not(x1: PairRDD[K,R]) = x1.mapValues(ring.negate)
//      def negate(x1: PairRDD[K,R]) = x1.mapValues(ring.negate)
//      def map[R1](c: PairRDD[K, R], f: (K, R) => (K, R1)): PairRDD[K, R1] = c.map { case (k, v) => f(k, v) }
//      def filter(c: PairRDD[K,R], f: (K,R) => Boolean): PairRDD[K,R] = c.filter { case (k, v) => f(k, v) }
//    }

  implicit def RDDCollection2[K:ClassTag, R:ClassTag](implicit ring: Ring[R]): Collection2[RDD[(K,R)],K,R] =
    new Collection2[RDD[(K,R)],K,R] {
      val innerRing = ring
      def zero = ??? // todo - need Spark context do initialize empty RDD but then cant serialize. however shouldnt ever need emptyRDD in practice.
      def add(x1: RDD[(K,R)], x2: RDD[(K,R)]) =
        x1.union(x2.rdd).groupByKey.mapValues(_.reduce(ring.add))
      def not(x1: RDD[(K,R)]) = x1.mapValues(ring.negate)
      def negate(x1: RDD[(K,R)]) = x1.mapValues(ring.negate)
//      def filter(c: RDD[(K,R)], f: (K,R) => Boolean): RDD[(K,R)] = c.filter { case (k, v) => f(k, v) }
    }

//  implicit def MapCollection[K, R](implicit ring: Ring[R]): Collection[Map, K, R] =
//    new Collection[Map, K, R] {
//      val innerRing = ring
//      def zero: Map[K, R] = Map.empty[K,R]
//      def add(t1: Map[K, R], t2: Map[K, R]): Map[K, R] =
//        t1 ++ t2.map { case (k, v) => k -> ring.add(v, t1.getOrElse(k, ring.zero)) }
//      def not(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.not(v)) }
//      def negate(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.negate(v)) }
//      def map[R1](c: Map[K, R], f: (K, R) => (K, R1)): Map[K, R1] = c.map { case (k, v) => f(k, v) }
//      def filter(c: Map[K, R], f: (K, R) => Boolean): Map[K, R] = c.filter { case (k, v) => f(k, v) }
//    }

  implicit def MapCollection2[K, R](implicit ring: Ring[R]): Collection2[Map[K,R], K, R] =
    new Collection2[Map[K,R], K, R] {
      val innerRing = ring
      def zero: Map[K, R] = Map.empty[K,R]
      def add(t1: Map[K, R], t2: Map[K, R]): Map[K, R] =
        t1 ++ t2.map { case (k, v) => k -> ring.add(v, t1.getOrElse(k, ring.zero)) }
      def not(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.not(v)) }
      def negate(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.negate(v)) }
//      def filter(c: Map[K, R], f: (K, R) => Boolean): Map[K, R] = c.filter { case (k, v) => f(k, v) }
    }
}

object Multiply {

  def instance[T1,T2,O](f: (T1,T2) => O): Multiply[T1, T2, O] = new Multiply[T1,T2,O] {
    def apply(v1: T1, v2: T2): O = f(v1,v2)
  }

  implicit def NumericMultiply[N](implicit num: Numeric[N]): Multiply[N,N,N] =
    instance { (t1,t2) => num.times(t1,t2) }

  implicit def mapMapMultiply[K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[Map[K, R1], Map[K, R2], Map[K, O]] =
    instance { (t1,t2) =>
      t1 map { case (k1, v1) =>
        k1 -> dot(v1, t2.getOrElse(k1, ring.zero))
      }
    }

  implicit def rddRddMultiply[K: ClassTag, R1: ClassTag, R2, O]
  (implicit dot: Dot[R1, R2, O]): Multiply[PairRDD[K, R1], PairRDD[K, R2], PairRDD[K, O]] =
    instance { (t1,t2) =>
      t1.join(t2) map { case (k, v) => k -> dot(v._1, v._2) }
    }

  implicit def rddMapMultiply[K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[PairRDD[K, R1], Map[K, R2], PairRDD[K, O]] =
    instance { case (t1,t2) =>
      t1.map { case (k, v) =>
        k -> dot(v, t2.getOrElse(k, ring.zero))
      }
    }

  implicit def mapRddMultiply[K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], ring: Ring[R1]): Multiply[Map[K, R1], PairRDD[K, R2], PairRDD[K, O]] =
    instance { (t1,t2) =>
      t2.map { case (k, v) =>
        k -> dot(t1.getOrElse(k, ring.zero),v)
      }
    }

  implicit def infMultiply[C[_, _], K, K1 >: K, R1, R2, O]
  (implicit dot: Dot[R1, R2, O], coll: Collection[C, K, R1]): Multiply[C[K, R1], K1 => R2, C[K, O]] =
    instance { (t1,t2) =>
      coll.map(t1, (k: K, v: R1) => k -> dot(v, t2(k)))
    }

  //todo - enable any kind of tuple multiplication?

}

object Dot {

  def instance[T1,T2,O](f: (T1,T2) => O): Dot[T1, T2, O] = new Dot[T1,T2,O] {
    def apply(v1: T1, v2: T2): O = f(v1,v2)
  }

  implicit def numericDot[N](implicit num: Numeric[N]): Dot[N, N, N] = instance { (t1,t2) => num.times(t1,t2) }

  implicit def mapMapDot[K1, K2, R1, R2, O]
  (implicit dot: Dot[R1, R2, O]): Dot[Map[K1, R1], Map[K2, R2], Map[(K1, K2), O]] =
    instance { (t1,t2) =>
      t1.flatMap { case (k1, v1) =>
        t2.map { case (k2, v2) => (k1, k2) -> dot(v1, v2) }
      }
    }

  implicit def rddRddDot[K1, K2, R1, R2, O](implicit dot: Dot[R1, R2, O]):
  Dot[PairRDD[K1, R1], PairRDD[K2, R2], PairRDD[(K1, K2), O]] =
    instance { (t1,t2) =>
      t1.cartesian(t2).map { case ((k1, v1), (k2, v2)) => (k1, k2) -> dot(v1, v2) }
    }

  implicit def pushDownDotLeft[C[_, _], K, R, R1: NonCollectionRing, O]
  (implicit collection: Collection[C, K, R], dot: Dot[R, R1, O]): Dot[C[K, R], R1, C[K, O]] =
    instance { (t1,t2) =>
      collection.map(t1, { case (k, v) => (k, dot(v, t2)) })
    }

  implicit def pushDownDotRight[C[_, _], K, R, R1: NonCollectionRing, O]
  (implicit collection: Collection[C, K, R], dot: Dot[R1, R, O]): Dot[R1, C[K, R], C[K, O]] =
    instance { (t1,t2) =>
      collection.map(t2, { case (k, v) => (k, dot(t1, v)) })
    }

}

object Join {

  implicit def rddRddJoin[
    K: ClassTag, K1 <: HList, K2 <: HList, K12 <: HList, R1, R2, O
  ](implicit dot: Dot[R1,R2,O], prepend: Prepend.Aux[K1,K2,K12]): Join[PairRDD[K::K1,R1],PairRDD[K::K2,R2],PairRDD[K::K12,O]] =
      new Join[PairRDD[K::K1,R1],PairRDD[K::K2,R2],PairRDD[K::K12,O]] {
      def apply(v1: PairRDD[K::K1,R1], v2: PairRDD[K::K2,R2]): PairRDD[K::K12,O] = {
        val left = v1.map { case (k::k1,r1) => (k,(k1,r1)) }
        val right = v2.map { case (k::k2,r2) => (k,(k2,r2)) }
        left.join(right).map { case (k,((k1,r1),(k2,r2))) => ((k::(prepend(k1,k2))),dot(r1,r2)) }
      }
    }
}

object Sum {

  def instance[T,O](f: T => O): Sum[T,O] = new Sum[T,O] {
    def apply(v1: T): O = f(v1)
  }

  implicit def MapSum[K, R](implicit ring: Ring[R]): Sum[Map[K,R],R] =
    instance { _.values.reduce(ring.add) }

  implicit def RddNumericSum[K : ClassTag, N : ClassTag](implicit ring: NumericRing[N]): Sum[PairRDD[K,N],N] =
    instance { _.values.reduce(ring.add) }

  implicit def RddMapSum[K : ClassTag, K1 : ClassTag, R1 : ClassTag]
  (implicit ring: Ring[R1], tag: ClassTag[Map[K1,R1]]): Sum[PairRDD[K,Map[K1,R1]],PairRDD[K1, R1]] =
    instance { v1 =>
      val flatMapped = new PairRDDFunctions[K1, R1](v1.values.flatMap(x => x))
      flatMapped.reduceByKey(ring.add)
    }
}


object Group {

  implicit def RddGroup[K1:ClassTag,K2 <: HList,R](implicit ring: Ring[R]): Group[PairRDD[K1::K2,R],PairRDD[K1::Map[K2,R]::HNil,Int]] =
    new Group[PairRDD[K1::K2,R],PairRDD[K1::Map[K2,R]::HNil,Int]] {
      def apply(v1: PairRDD[K1::K2,R]): PairRDD[K1::Map[K2,R]::HNil,Int] = {
        //todo - this is wrong - it doesnt aggregate - the toMap just does overwrites
        val grouped = v1.groupBy(_._1.head).mapValues[Map[K2,R]] { _.map { case (_::k2,v) => (k2,v) } toMap }
        grouped.map[(K1::Map[K2,R]::HNil,Int)] { case (k1,k2s) => (k1::k2s::HNil,1) }
      }
    }
}


//  implicit def Tuple2Ring[R1,R2](implicit r1: Ring[R1], r2: Ring[R2]): NonCollectionRing[(R1,R2)] =
//    new NonCollectionRing[(R1,R2)] {
//      def zero: (R1,R2) = (r1.zero,r2.zero)
//      def add(t1: (R1,R2), t2: (R1,R2)): (R1,R2) = (r1.add(t1._1, t2._1),r2.add(t1._2,t2._2))
//      def not(t1: (R1,R2)): (R1,R2) = (r1.not(t1._1),r2.not(t1._2))
//      def negate(t1: (R1,R2)): (R1,R2) = (r1.negate(t1._1),r2.negate(t1._2))
//    }
//
//  implicit def Tuple3Ring[R1,R2,R3](implicit r1: Ring[R1], r2: Ring[R2], r3: Ring[R3]): NonCollectionRing[(R1,R2,R3)] =
//    new NonCollectionRing[(R1,R2,R3)] {
//      def zero: (R1,R2,R3) = (r1.zero,r2.zero,r3.zero)
//      def add(t1: (R1,R2,R3), t2: (R1,R2,R3)): (R1,R2,R3) = (r1.add(t1._1, t2._1),r2.add(t1._2,t2._2),r3.add(t1._3,t2._3))
//      def not(t1: (R1,R2,R3)): (R1,R2,R3) = (r1.not(t1._1),r2.not(t1._2),r3.not(t1._3))
//      def negate(t1: (R1,R2,R3)): (R1,R2,R3) = (r1.negate(t1._1),r2.negate(t1._2),r3.negate(t1._3))
//    }

//  implicit def tuple2NumericDot[R1,R2,N : NumericRing,O1,O2](implicit dot1: Dot[R1,N,O1], dot2: Dot[R2,N,O2]):
//  Dot[(R1,R2),N,(O1,O2)] = new Dot[(R1,R2),N,(O1,O2)] {
//    def apply(v1: (R1, R2), v2: N): (O1, O2) = (dot1(v1._1, v2), dot2(v1._2, v2))
//  }
//
//  implicit def numericTuple2Dot[R1,R2,N : NumericRing,O1,O2](implicit dot1: Dot[N,R1,O1], dot2: Dot[N,R2,O2]):
//  Dot[N,(R1,R2),(O1,O2)] = new Dot[N,(R1,R2),(O1,O2)] {
//    def apply(v1: N, v2: (R1, R2)): (O1, O2) = (dot1(v1, v2._1), dot2(v1, v2._2))
//  }
//
//  implicit def tuple3NumericDot[R1,R2,R3,N : NumericRing,O1,O2,O3]
//  (implicit dot1: Dot[R1,N,O1], dot2: Dot[R2,N,O2], dot3: Dot[R3,N,O3]): Dot[(R1,R2,R3),N,(O1,O2,O3)] =
//    new Dot[(R1,R2,R3),N,(O1,O2,O3)] {
//      def apply(v1: (R1, R2, R3), v2: N): (O1, O2, O3) = (dot1(v1._1, v2), dot2(v1._2, v2), dot3(v1._3, v2))
//    }
//
//
//  implicit def numericTuple3Dot[R1,R2,R3,N : NumericRing,O1,O2,O3]
//  (implicit dot1: Dot[N,R1,O1], dot2: Dot[N,R2,O2], dot3: Dot[N,R3,O3]):
//  Dot[N,(R1,R2,R3),(O1,O2,O3)] = new Dot[N,(R1,R2,R3),(O1,O2,O3)] {
//    def apply(v1: N, v2: (R1,R2,R3)): (O1,O2,O3) = (dot1(v1,v2._1),dot2(v1,v2._2),dot3(v1,v2._3))
//  }

//
//  implicit def rdd2Rdd2Join[K: ClassTag, K1: ClassTag, K2: ClassTag, R1, R2, O](implicit dot: Dot[R1,R2,O]):
//    Join[PairRDD[(K,K1),R1],PairRDD[(K,K2),R2],PairRDD[(K,(K1,K2)),O]] =
//      new Join[PairRDD[(K,K1),R1],PairRDD[(K,K2),R2],PairRDD[(K,(K1,K2)),O]] {
//        def apply(v1: PairRDD[(K,K1),R1], v2: PairRDD[(K,K2),R2]): PairRDD[(K,(K1,K2)),O] = {
//          val left = v1.map { case ((k,k1),r1) => (k,(k1,r1)) }
//          val right = v2.map { case ((k,k2),r2) => (k,(k2,r2)) }
//          left.join(right).map { case (k,((k1,r1),(k2,r2))) => ((k,(k1,k2)),dot(r1,r2)) }
//        }
//      }
//
//  implicit def rdd2Rdd3Join[K: ClassTag, K11: ClassTag, K21: ClassTag, K22: ClassTag, R1, R2, O](implicit dot: Dot[R1,R2,O]):
//  Join[PairRDD[(K,K11),R1],PairRDD[(K,K21,K22),R2],PairRDD[(K,(K11,(K21,K22))),O]] =
//    new Join[PairRDD[(K,K11),R1],PairRDD[(K,K21,K22),R2],PairRDD[(K,(K11,(K21,K22))),O]] {
//      def apply(v1: PairRDD[(K,K11),R1], v2: PairRDD[(K,K21,K22),R2]): PairRDD[(K,(K11,(K21,K22))),O] = {
//        val left = v1.map { case ((k,k11),r1) => (k,(k11,r1)) }
//        val right = v2.map { case ((k,k21,k22),r2) => (k,(k21,k22,r2)) }
//        left.join(right).map { case (k,((k11,r1),(k21,k22,r2))) => ((k,(k11,(k21,k22))),dot(r1,r2)) }
//      }
//    }
//
//  implicit def rdd3Rdd2Join[
//  K: ClassTag, K11: ClassTag, K12: ClassTag, K21: ClassTag, R1, R2, O
//  ](implicit dot: Dot[R1,R2,O]): Join[PairRDD[(K,K11,K12),R1],PairRDD[(K,K21),R2],PairRDD[(K,((K11,K12),K21)),O]] =
//    new Join[PairRDD[(K,K11,K12),R1],PairRDD[(K,K21),R2],PairRDD[(K,((K11,K12),K21)),O]] {
//      def apply(v1: PairRDD[(K,K11,K12),R1], v2: PairRDD[(K,K21),R2]): PairRDD[(K,((K11,K12),K21)),O] = {
//        val left = v1.map { case ((k,k11,k12),r1) => (k,(k11,k12,r1)) }
//        val right = v2.map { case ((k,k21),r2) => (k,(k21,r2)) }
//        left.join(right).map { case (k,((k11,k12,r1),(k21,r2))) => ((k,((k11,k12),k21)),dot(r1,r2)) }
//      }
//    }
//
//  implicit def rdd3Rdd3Join[
//    K: ClassTag, K11: ClassTag, K12: ClassTag, K21: ClassTag, K22: ClassTag, R1, R2, O
//  ](implicit dot: Dot[R1,R2,O]): Join[PairRDD[(K,K11,K12),R1],PairRDD[(K,K21,K22),R2],PairRDD[(K,((K11,K12),(K21,K22))),O]] =
//    new Join[PairRDD[(K,K11,K12),R1],PairRDD[(K,K21,K22),R2],PairRDD[(K,((K11,K12),(K21,K22))),O]] {
//      def apply(v1: PairRDD[(K,K11,K12),R1], v2: PairRDD[(K,K21,K22),R2]): PairRDD[(K,((K11,K12),(K21,K22))),O] = {
//        val left = v1.map { case ((k,k11,k12),r1) => (k,(k11,k12,r1)) }
//        val right = v2.map { case ((k,k21,k22),r2) => (k,(k21,k22,r2)) }
//        left.join(right).map { case (k,((k11,k12,r1),(k21,k22,r2))) => ((k,((k11,k12),(k21,k22))),dot(r1,r2)) }
//      }
//    }


//  implicit def MapGroup2[K1,K2,R](implicit ring: Ring[R]): Group[Map[(K1,K2),R],Map[(K1,Map[K2,R]),Int]] =
//    new Group[Map[(K1,K2),R],Map[(K1,Map[K2,R]),Int]] {
//      def apply(v1: Map[(K1,K2),R]): Map[(K1,Map[K2,R]),Int] = {
//        val grouped = v1.groupBy(_._1._1).mapValues(_.toList.map { case ((k1,k2),v) => (k2,v) }.toMap)
//        grouped.toList.zip(Stream.from(1,0)).toMap
//      }
//    }
//
//  implicit def MapGroup3[K1,K2,K3,R](implicit ring: Ring[R]): Group[Map[(K1,K2,K3),R],Map[(K1,Map[(K2,K3),R]),Int]] =
//    new Group[Map[(K1,K2,K3),R],Map[(K1,Map[(K2,K3),R]),Int]] {
//      def apply(v1: Map[(K1,K2,K3),R]): Map[(K1,Map[(K2,K3),R]),Int] = {
//        val grouped = v1.groupBy(_._1._1).mapValues(_.toList.map { case ((k1,k2,k3),v) => ((k2,k3),v) }.toMap)
//        grouped.toList.zip(Stream.from(1,0)).toMap
//      }
//    }

//  implicit def RddGroup2[K1:ClassTag,K2,R](implicit ring: Ring[R]): Group[PairRDD[(K1,K2),R],PairRDD[(K1,Map[K2,R]),Int]] =
//    new Group[PairRDD[(K1,K2),R],PairRDD[(K1,Map[K2,R]),Int]] {
//      def apply(v1: PairRDD[(K1,K2),R]): PairRDD[(K1,Map[K2,R]),Int] = {
//        val grouped = v1.groupBy(_._1._1).mapValues[Map[K2,R]](_.map { case ((_,k2),v) => (k2,v) }.toMap)
//        grouped.map { case (k,v) => ((k,v),1) }
//      }
//    }
//
//  implicit def RddGroup3[K1:ClassTag,K2,K3,R](implicit ring: Ring[R]):
//  Group[PairRDD[(K1,K2,K3),R],PairRDD[(K1,Map[(K2,K3),R]),Int]] =
//    new Group[PairRDD[(K1,K2,K3),R],PairRDD[(K1,Map[(K2,K3),R]),Int]] {
//      def apply(v1: PairRDD[(K1,K2,K3),R]): PairRDD[(K1,Map[(K2,K3),R]),Int] = {
//        val grouped = v1.groupBy(_._1._1).mapValues[Map[(K2,K3),R]](_.map { case ((_,k2,k3),v) => ((k2,k3),v) }.toMap)
//        grouped.map { case (k,v) => ((k,v),1) }
//      }
//    }
