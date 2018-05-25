package slender

trait Ring[R] extends Serializable {
  def zero: R
  def add(x1: R, x2: R): R
}

trait Collection[C[_,_],K,R] extends Ring[C[K,R]] {
  def sum(c: C[K,R]): R
  def map[R1](c: C[K,R], f: (K,R) => (K,R1)): C[K,R1]
}

trait Dot[T1,T2,O] extends ((T1,T2) => O) with Serializable

trait Multiply[T1,T2,O] extends ((T1,T2) => O) with Serializable

