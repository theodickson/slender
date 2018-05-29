package slender

import scala.reflect.ClassTag
import scala.collection.immutable.Map

trait RingImplicits {

  implicit def NumericRing[N](implicit ev: Numeric[N]): NumericRing[N] = new NumericRing[N] { val num = ev }

  implicit def Tuple2Ring[R1,R2](implicit r1: Ring[R1], r2: Ring[R2]): NonCollectionRing[(R1,R2)] = new NonCollectionRing[(R1,R2)] {
    def zero: (R1,R2) = (r1.zero,r2.zero)
    def add(t1: (R1,R2), t2: (R1,R2)): (R1,R2) = (r1.add(t1._1, t2._1),r2.add(t1._2,t2._2))
    def not(t1: (R1,R2)): (R1,R2) = (r1.not(t1._1),r2.not(t1._2))
    def negate(t1: (R1,R2)): (R1,R2) = (r1.negate(t1._1),r2.negate(t1._2))
  }

  implicit def Tuple3Ring[R1,R2,R3](implicit r1: Ring[R1], r2: Ring[R2], r3: Ring[R3]): NonCollectionRing[(R1,R2,R3)] = new NonCollectionRing[(R1,R2,R3)] {
    def zero: (R1,R2,R3) = (r1.zero,r2.zero,r3.zero)
    def add(t1: (R1,R2,R3), t2: (R1,R2,R3)): (R1,R2,R3) = (r1.add(t1._1, t2._1),r2.add(t1._2,t2._2),r3.add(t1._3,t2._3))
    def not(t1: (R1,R2,R3)): (R1,R2,R3) = (r1.not(t1._1),r2.not(t1._2),r3.not(t1._3))
    def negate(t1: (R1,R2,R3)): (R1,R2,R3) = (r1.negate(t1._1),r2.negate(t1._2),r3.negate(t1._3))
  }

  implicit def RDDCollection[K: ClassTag, R: ClassTag](implicit ring: Ring[R]): Collection[PairRDD, K, R] =
    new Collection[PairRDD, K, R] {
      def zero = ??? // todo - need Spark context do initialize empty RDD but then cant serialize. however shouldnt ever need emptyRDD in practice.
      def add(x1: PairRDD[K, R], x2: PairRDD[K, R]) =
        x1.union(x2.rdd).groupByKey.mapValues(_.reduce(ring.add))

      def not(x1: PairRDD[K,R]) = x1.mapValues(ring.negate)

      def negate(x1: PairRDD[K,R]) = x1.mapValues(ring.negate)

      def sum(c: PairRDD[K, R]): R = c.values.reduce(ring.add) //

      def map[R1](c: PairRDD[K, R], f: (K, R) => (K, R1)): PairRDD[K, R1] = c.map { case (k, v) => f(k, v) }
    }

  implicit def MapCollection[K, R](implicit ring: Ring[R]): Collection[Map, K, R] = new Collection[Map, K, R] {
    def zero: Map[K, R] = Map.empty[K,R]

    def add(t1: Map[K, R], t2: Map[K, R]): Map[K, R] =
      t1 ++ t2.map { case (k, v) => k -> ring.add(v, t1.getOrElse(k, ring.zero)) }

    def not(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.not(v)) }//t1.mapValues(ring.not)

    def negate(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.negate(v)) } //Values(ring.negate)

    def sum(c: Map[K, R]): R = c.values.reduce(ring.add)

    def map[R1](c: Map[K, R], f: (K, R) => (K, R1)): Map[K, R1] = c.map { case (k, v) => f(k, v) }
  }
}

trait DotImplicits {
//  implicit def mapNumericDot[K : ClassTag, R : ClassTag, N : NumericRing, O](implicit dot: Dot[R,N,O]):
//    Dot[Map[K,R],N,Map[K,O]] = new Dot[Map[K,R],N,Map[K,O]] {
//    def apply(v1: Map[K,R], v2: N): Map[K,O] = v1.map { case (k,v) => k -> dot(v,v2) }
//  }

//  implicit def numericMapDot[K : ClassTag, R : ClassTag, N : NumericRing, O](implicit dot: Dot[N,R,O]):
//  Dot[N,Map[K,R],Map[K,O]] = new Dot[N,Map[K,R],Map[K,O]] {
//    def apply(v1: N, v2: Map[K,R]): Map[K,O] = v2.map { case (k,v) => k -> dot(v1,v) }

//  implicit def collectionNumericDot[C[_,_], K : ClassTag, R : ClassTag, N : NumericRing, O]
//  (implicit collection: Collection[C,K,R], dot: Dot[R,N,O]): Dot[C[K,R],N,C[K,O]] = new Dot[C[K,R],N,C[K,O]] {
//    def apply(v1: C[K,R], v2: N): C[K,O] = collection.map(v1,{ case (k,v) => k -> dot(v,v2)})
//  }
//
//  implicit def numericCollectionDot[C[_,_], K : ClassTag, R : ClassTag, N : NumericRing, O]
//  (implicit collection: Collection[C,K,R],dot: Dot[N,R,O]): Dot[N,C[K,R],C[K,O]] = new Dot[N,C[K,R],C[K,O]] {
//    def apply(v1: N, v2: C[K, R]): C[K, O] = collection.map(v2, { case (k, v) => k -> dot(v1, v) })
//  }

  implicit def NumericDot[N](implicit num: Numeric[N]): Dot[N,N,N] = new Dot[N,N,N] {
    def apply(t1: N, t2: N): N = num.times(t1,t2)
  }

  implicit def mapMapDot[K1, K2, R1, R2, O](implicit recur: Dot[R1, R2, O]): Dot[Map[K1, R1], Map[K2, R2], Map[(K1, K2), O]] =
    new Dot[Map[K1, R1], Map[K2, R2], Map[(K1, K2), O]] {
      def apply(t1: Map[K1, R1], t2: Map[K2, R2]): Map[(K1, K2), O] = t1.flatMap { case (k1, v1) =>
        t2.map { case (k2, v2) => (k1, k2) -> recur(v1, v2) }
      }
    }

  implicit def rddRddDot[K1, K2, R1, R2, O](implicit recur: Dot[R1, R2, O]):
    Dot[PairRDD[K1, R1], PairRDD[K2, R2], PairRDD[(K1,K2),O]] =
    new Dot[PairRDD[K1, R1], PairRDD[K2, R2], PairRDD[(K1, K2), O]] {
      def apply(t1: PairRDD[K1, R1], t2: PairRDD[K2, R2]): PairRDD[(K1, K2), O] =
        t1.cartesian(t2).map { case ((k1, v1), (k2, v2)) => (k1, k2) -> recur(v1, v2) }
    }

  implicit def tuple2NumericDot[R1,R2,N : NumericRing,O1,O2](implicit dot1: Dot[R1,N,O1], dot2: Dot[R2,N,O2]):
    Dot[(R1,R2),N,(O1,O2)] = new Dot[(R1,R2),N,(O1,O2)] {
    def apply(v1: (R1, R2), v2: N): (O1, O2) = (dot1(v1._1, v2), dot2(v1._2, v2))
  }

  implicit def numericTuple2Dot[R1,R2,N : NumericRing,O1,O2](implicit dot1: Dot[N,R1,O1], dot2: Dot[N,R2,O2]):
    Dot[N,(R1,R2),(O1,O2)] = new Dot[N,(R1,R2),(O1,O2)] {
    def apply(v1: N, v2: (R1, R2)): (O1, O2) = (dot1(v1, v2._1), dot2(v1, v2._2))
  }

  implicit def tuple3NumericDot[R1,R2,R3,N : NumericRing,O1,O2,O3]
  (implicit dot1: Dot[R1,N,O1], dot2: Dot[R2,N,O2], dot3: Dot[R3,N,O3]): Dot[(R1,R2,R3),N,(O1,O2,O3)] =
    new Dot[(R1,R2,R3),N,(O1,O2,O3)] {
      def apply(v1: (R1, R2, R3), v2: N): (O1, O2, O3) = (dot1(v1._1, v2), dot2(v1._2, v2), dot3(v1._3, v2))
    }


  implicit def numericTuple3Dot[R1,R2,R3,N : NumericRing,O1,O2,O3]
  (implicit dot1: Dot[N,R1,O1], dot2: Dot[N,R2,O2], dot3: Dot[N,R3,O3]):
  Dot[N,(R1,R2,R3),(O1,O2,O3)] = new Dot[N,(R1,R2,R3),(O1,O2,O3)] {
    def apply(v1: N, v2: (R1,R2,R3)): (O1,O2,O3) = (dot1(v1,v2._1),dot2(v1,v2._2),dot3(v1,v2._3))
  }

  implicit def pushDownDotLeft[C[_,_],K,R,R1 : NonCollectionRing,O]
  (implicit collection: Collection[C,K,R], recur: Dot[R,R1,O]): Dot[C[K,R],R1,C[K,O]] =
    new Dot[C[K,R],R1,C[K,O]] {
      def apply(t1: C[K, R], t2: R1): C[K, O] = collection.map(t1,{ case (k,v) => (k,recur(v,t2)) })
    }

  implicit def pushDownDotRight[C[_,_],K,R,R1 : NonCollectionRing,O]
  (implicit collection: Collection[C,K,R], recur: Dot[R1,R,O]): Dot[R1,C[K,R],C[K,O]] =
    new Dot[R1,C[K,R],C[K,O]] {
      def apply(t1: R1, t2: C[K, R]): C[K, O] = collection.map(t2,{ case (k,v) => (k,recur(t1,v)) })
    }


}

trait MultiplyImplicits {

  implicit def NumericMultiply[N](implicit num: Numeric[N]): Multiply[N,N,N] = new Multiply[N,N,N] {
    def apply(t1: N, t2: N): N = num.times(t1,t2)
  }

  implicit def mapMapMultiply[K, R1, R2, O](implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[Map[K, R1], Map[K, R2], Map[K, O]] =
    new Multiply[Map[K, R1], Map[K, R2], Map[K, O]] {
      def apply(t1: Map[K, R1], t2: Map[K, R2]): Map[K, O] = t1 map { case (k1, v1) =>
        k1 -> dot(v1, t2.getOrElse(k1, ring.zero))
      }
    }

  implicit def rddRddMultiply[K: ClassTag, R1: ClassTag, R2, O](implicit dot: Dot[R1, R2, O]): Multiply[PairRDD[K, R1], PairRDD[K, R2], PairRDD[K, O]] =
    new Multiply[PairRDD[K, R1], PairRDD[K, R2], PairRDD[K, O]] {
      def apply(t1: PairRDD[K, R1], t2: PairRDD[K, R2]): PairRDD[K, O] =
        t1.join(t2) map { case (k, v) => k -> dot(v._1, v._2) }
    }

  implicit def rddMapMultiply[K, R1, R2, O](implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[PairRDD[K, R1], Map[K, R2], PairRDD[K, O]] =
    new Multiply[PairRDD[K, R1], Map[K, R2], PairRDD[K, O]] {
      def apply(t1: PairRDD[K, R1], t2: Map[K, R2]): PairRDD[K, O] =
        t1.map { case (k, v) => k -> dot(v, t2.getOrElse(k, ring.zero)) }
    }

  implicit def mapRddMultiply[K, R1, R2, O](implicit dot: Dot[R1, R2, O], ring: Ring[R1]): Multiply[Map[K, R1], PairRDD[K, R2], PairRDD[K, O]] =
    new Multiply[Map[K, R1], PairRDD[K, R2], PairRDD[K, O]] {
      def apply(t1: Map[K, R1], t2: PairRDD[K, R2]): PairRDD[K, O] =
        t2.map { case (k, v) => k -> dot(t1.getOrElse(k, ring.zero),v) }
    }

  implicit def infMultiply[C[_, _], K, R1, R2, O](implicit dot: Dot[R1, R2, O], coll: Collection[C, K, R1]): Multiply[C[K, R1], K => R2, C[K, O]] =
    new Multiply[C[K, R1], K => R2, C[K, O]] {
      def apply(t1: C[K, R1], t2: K => R2): C[K, O] = coll.map(t1, (k: K, v: R1) => k -> dot(v, t2(k)))
    }

  //todo - enable any kind of tuple multiplication?

//  implicit def pushDownDotLeft[K, R, O](implicit recur: Dot[R, Int, O]): Dot[Map[K, R], Int, Map[K, O]] =
//    new Dot[Map[K, R], Int, Map[K, O]] {
//      def apply(t1: Map[K, R], t2: Int): Map[K, O] = t1 map { case (k,v) => (k,recur(v, t2)) }
//    }
//
//  implicit def pushDownDotRight[K, R, O](implicit recur: Dot[Int, R, O]): Dot[Int, Map[K, R], Map[K, O]] =
//    new Dot[Int, Map[K, R], Map[K, O]] {
//      def apply(t1: Int, t2: Map[K, R]): Map[K, O] = t2 map { case (k,v) => (k,recur(t1, v)) }
//    }


}

trait AlgebraImplicits extends RingImplicits with DotImplicits with MultiplyImplicits
