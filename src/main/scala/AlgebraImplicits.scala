package slender

import scala.reflect.ClassTag
import scala.collection.immutable.Map

trait AlgebraBaseImplicits {

  implicit def NumericRing[N](implicit num: Numeric[N]): Ring[N] = new Ring[N] {
    def zero: N = num.fromInt(0)
    def one: N = num.fromInt(1)
    def add(t1: N, t2: N): N = num.plus(t1,t2)
    def not(t1: N): N = if (t1 == zero) one else zero
    def negate(t1: N): N = num.negate(t1)
  }

//  implicit object IntRing extends Ring[Int] {
//    def zero = 0
//    def add(t1: Int, t2: Int) = t1 + t2
//    def not(t1: Int): Int = if (t1 == 0) 1 else 0
//    def negate(t1: Int): Int = -t1
//  }

  implicit def NumericDot[N](implicit num: Numeric[N]): Dot[N,N,N] = new Dot[N,N,N] {
    def apply(t1: N, t2: N): N = num.times(t1,t2)
  }

  implicit def NumericMultiply[N](implicit num: Numeric[N]): Multiply[N,N,N] = new Multiply[N,N,N] {
    def apply(t1: N, t2: N): N = num.times(t1,t2)
  }

//  implicit object IntDot extends Dot[Int, Int, Int] {
//    def apply(t1: Int, t2: Int): Int = t1 * t2
//  }
//
//  implicit object IntMultiply extends Multiply[Int, Int, Int] {
//    def apply(t1: Int, t2: Int): Int = t1 * t2
//  }

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

trait AlgebraTupleImplicits {

  implicit def Tuple2Ring[R1,R2](implicit r1: Ring[R1], r2: Ring[R2]): Ring[(R1,R2)] = new Ring[(R1,R2)] {
    def zero: (R1,R2) = (r1.zero,r2.zero)
    def add(t1: (R1,R2), t2: (R1,R2)): (R1,R2) = (r1.add(t1._1, t2._1),r2.add(t1._2,t2._2))
    def not(t1: (R1,R2)): (R1,R2) = (r1.not(t1._1),r2.not(t1._2))
    def negate(t1: (R1,R2)): (R1,R2) = (r1.negate(t1._1),r2.negate(t1._2))
  }

  implicit def Tuple3Ring[R1,R2,R3](implicit r1: Ring[R1], r2: Ring[R2], r3: Ring[R3]): Ring[(R1,R2,R3)] = new Ring[(R1,R2,R3)] {
    def zero: (R1,R2,R3) = (r1.zero,r2.zero,r3.zero)
    def add(t1: (R1,R2,R3), t2: (R1,R2,R3)): (R1,R2,R3) = (r1.add(t1._1, t2._1),r2.add(t1._2,t2._2),r3.add(t1._3,t2._3))
    def not(t1: (R1,R2,R3)): (R1,R2,R3) = (r1.not(t1._1),r2.not(t1._2),r3.not(t1._3))
    def negate(t1: (R1,R2,R3)): (R1,R2,R3) = (r1.negate(t1._1),r2.negate(t1._2),r3.negate(t1._3))
  }

}

trait AlgebraMultiplicationImplicits {
  implicit def rddRddMultiply[K: ClassTag, R1: ClassTag, R2, O](implicit dot: Dot[R1, R2, O]): Multiply[PairRDD[K, R1], PairRDD[K, R2], PairRDD[K, O]] =
    new Multiply[PairRDD[K, R1], PairRDD[K, R2], PairRDD[K, O]] {
      def apply(t1: PairRDD[K, R1], t2: PairRDD[K, R2]): PairRDD[K, O] =
        t1.rdd.join(t2.rdd) map { case (k, v) => k -> dot(v._1, v._2) }
    }

  implicit def rddMapMultiply[K, R1, R2, O](implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[PairRDD[K, R1], Map[K, R2], PairRDD[K, O]] =
    new Multiply[PairRDD[K, R1], Map[K, R2], PairRDD[K, O]] {
      def apply(t1: PairRDD[K, R1], t2: Map[K, R2]): PairRDD[K, O] =
        t1.rdd.map { case (k, v) => k -> dot(v, t2.getOrElse(k, ring.zero)) }
    }

  implicit def mapMapMultiply[K, R1, R2, O](implicit dot: Dot[R1, R2, O], ring: Ring[R2]): Multiply[Map[K, R1], Map[K, R2], Map[K, O]] =
    new Multiply[Map[K, R1], Map[K, R2], Map[K, O]] {
      def apply(t1: Map[K, R1], t2: Map[K, R2]): Map[K, O] = t1 map { case (k1, v1) =>
        k1 -> dot(v1, t2.getOrElse(k1, ring.zero))
      }
    }

  implicit def infMultiply[C[_, _], K, R1, R2, O](implicit dot: Dot[R1, R2, O], coll: Collection[C, K, R1]): Multiply[C[K, R1], K => R2, C[K, O]] =
    new Multiply[C[K, R1], K => R2, C[K, O]] {
      def apply(t1: C[K, R1], t2: K => R2): C[K, O] = coll.map(t1, (k: K, v: R1) => k -> dot(v, t2(k)))
    }

  implicit def pushDownDotLeft[K, R, O](implicit recur: Dot[R, Int, O]): Dot[Map[K, R], Int, Map[K, O]] =
    new Dot[Map[K, R], Int, Map[K, O]] {
      def apply(t1: Map[K, R], t2: Int): Map[K, O] = t1 map { case (k,v) => (k,recur(v, t2)) }
    }

  implicit def pushDownDotRight[K, R, O](implicit recur: Dot[Int, R, O]): Dot[Int, Map[K, R], Map[K, O]] =
    new Dot[Int, Map[K, R], Map[K, O]] {
      def apply(t1: Int, t2: Map[K, R]): Map[K, O] = t2 map { case (k,v) => (k,recur(t1, v)) }
    }

  implicit def productDots[K1, K2, R1, R2, O](implicit recur: Dot[R1, R2, O]): Dot[Map[K1, R1], Map[K2, R2], Map[(K1, K2), O]] =
    new Dot[Map[K1, R1], Map[K2, R2], Map[(K1, K2), O]] {
      def apply(t1: Map[K1, R1], t2: Map[K2, R2]): Map[(K1, K2), O] = t1.flatMap { case (k1, v1) =>
        t2.map { case (k2, v2) => (k1, k2) -> recur(v1, v2) }
      }
    }
}

trait AlgebraImplicits extends AlgebraBaseImplicits with AlgebraTupleImplicits with AlgebraMultiplicationImplicits
