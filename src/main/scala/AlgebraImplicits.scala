package slender

import scala.reflect.ClassTag
import scala.collection.immutable.Map

trait AlgebraImplicits {

  implicit object IntRing extends Ring[Int] {
    def zero = 0
    def add(t1: Int, t2: Int) = t1 + t2
    def not(t1: Int): Int = if (t1 == 0) 1 else 0
    def negate(t1: Int): Int = -t1
  }

  implicit object IntDot extends Dot[Int, Int, Int] {
    def apply(t1: Int, t2: Int): Int = t1 * t2
  }

  implicit object IntMultiply extends Multiply[Int, Int, Int] {
    def apply(t1: Int, t2: Int): Int = t1 * t2
  }

  implicit def rddToCollection[K: ClassTag, R: ClassTag](implicit ring: Ring[R]): Collection[PairRDD, K, R] =
    new Collection[PairRDD, K, R] {
      def zero = ??? // todo - need Spark context do initialize empty RDD but then cant serialize. however shouldnt ever need emptyRDD in practice.
      def add(x1: PairRDD[K, R], x2: PairRDD[K, R]) =
        x1.union(x2.rdd).groupByKey.mapValues(_.reduce(ring.add))

      def not(x1: PairRDD[K,R]) = x1.mapValues(ring.negate)

      def negate(x1: PairRDD[K,R]) = x1.mapValues(ring.negate)

      def sum(c: PairRDD[K, R]): R = c.values.reduce(ring.add)

      def map[R1](c: PairRDD[K, R], f: (K, R) => (K, R1)): PairRDD[K, R1] = c.map { case (k, v) => f(k, v) }
    }

  implicit def mapToCollection[K, R](implicit ring: Ring[R]): Collection[Map, K, R] = new Collection[Map, K, R] {
    def zero: Map[K, R] = Map.empty[K,R]

    def add(t1: Map[K, R], t2: Map[K, R]): Map[K, R] =
      t1 ++ t2.map { case (k, v) => k -> ring.add(v, t1.getOrElse(k, ring.zero)) }

    def not(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.not(v)) }//t1.mapValues(ring.not)

    def negate(t1: Map[K,R]): Map[K, R] = t1.map { case (k,v) => (k,ring.negate(v)) } //Values(ring.negate)

    def sum(c: Map[K, R]): R = c.values.reduce(ring.add)

    def map[R1](c: Map[K, R], f: (K, R) => (K, R1)): Map[K, R1] = c.map { case (k, v) => f(k, v) }
  }

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