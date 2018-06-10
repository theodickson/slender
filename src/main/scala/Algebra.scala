package slender

import scala.reflect.ClassTag

trait Ring[R] extends Serializable {
  def zero: R
  def add(x1: R, x2: R): R
  def not(x1: R): R
  def negate(x1: R): R
  def filterZeros(x1: R): R = x1
}

trait Collection[C[_,_],K,R] extends Ring[C[K,R]] {
  def innerRing: Ring[R]
  def sum[S](c: C[K,R])(implicit ev: Sum[C[K,R],S]): S
  def map[R1](c: C[K,R], f: (K,R) => (K,R1)): C[K,R1]
  def filter(c: C[K,R], f: (K,R) => Boolean): C[K,R]
  override def filterZeros(c: C[K,R]): C[K,R] = {
    val recursivelyFiltered = map(c, (k,r) => (k,innerRing.filterZeros(r)))
    filter(recursivelyFiltered, (k,r) => r != innerRing.zero)
  }
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

trait Sum[T,S] extends (T => S) with Serializable

trait Dot[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Multiply[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait OpImplicits {
  implicit class CollectionImplicits[C[_,_],K,R](c: C[K,R])(implicit coll: Collection[C,K,R]) {
    def filterZeros: C[K,R] = coll.filterZeros(c)
  }
}