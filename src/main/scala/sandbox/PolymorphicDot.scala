package sandbox

object PolymorphicDot {

  trait Dot[T1,T2,O] {
    def apply(t1: T1, t2: T2): O
  }

  implicit object IntDot extends Dot[Int,Int,Int] {
    def apply(t1: Int, t2: Int): Int = t1*t2
  }

  implicit def pushDownDotLeft[K,R,O](implicit recur: Dot[R,Int,O]): Dot[Map[K,R],Int,Map[K,O]] =
    new Dot[Map[K,R],Int,Map[K,O]] {
    def apply(t1: Map[K,R], t2: Int): Map[K,O] = t1 mapValues { v => recur(v,t2) }
  }

  implicit def pushDownDotRight[K,R,O](implicit recur: Dot[Int,R,O]): Dot[Int,Map[K,R],Map[K,O]] =
    new Dot[Int,Map[K,R],Map[K,O]] {
    def apply(t1: Int, t2: Map[K,R]): Map[K,O] = t2 mapValues { v => recur(t1,v) }
  }

  implicit def productDots[K1,K2,R1,R2,O](implicit recur: Dot[R1,R2,O]): Dot[Map[K1,R1],Map[K2,R2],Map[(K1,K2),O]] =
    new Dot[Map[K1,R1],Map[K2,R2],Map[(K1,K2),O]] {
      def apply(t1: Map[K1, R1], t2: Map[K2, R2]): Map[(K1,K2),O] = t1.flatMap { case (k1, v1) =>
        t2.map { case (k2, v2) => (k1, k2) -> recur(v1, v2) }
      }
    }

  def dot[T1,T2,O](x1: T1, x2: T2)(implicit ev: Dot[T1,T2,O]): O = ev(x1,x2)

  def main(args: Array[String]): Unit = {
    val map1 = Map(
      "a" -> 1,
      "b" -> 2,
      "c" -> 3
    )
    val map2 = Map(
      "a" -> Map("a" -> 1),
      "b" -> Map("b" -> 1)
    )
    val result = dot(dot(map1,map2),dot(map2,dot(1,dot(map2,map1))))
    println(result)
  }
}

//object PolymorphicDot {
//
//  trait Dot[T1,T2] {
//    type out <: Int
//    def apply(t1: T1, t2: T2): out
//  }
//
//  implicit object IntDot extends Dot[Int,Int] {
//    type out = Int
//    def apply(t1: Int, t2: Int): out = t1*t2
//  }
//
//  implicit def pushDownDotLeft[K,R](implicit recur: Dot[R,Int]): Dot[Map[K,R],Int] = new Dot[Map[K,R],Int] {
//    type out = Map[K,recur.out]
//    def apply(t1: Map[K,R], t2: Int): out = t1 mapValues { v => recur(v,t2) }
//  }
//
//  implicit def pushDownDotRight[K,R](implicit recur: Dot[Int,R]): Dot[Int,Map[K,R]] = new Dot[Int,Map[K,R]] {
//    type out = Map[K,recur.out]
//    def apply(t1: Int, t2: Map[K,R]): out = t2 mapValues { v => recur(t1,v) }
//  }
//
//  implicit def productDots[K1,K2,R1,R2](implicit recur: Dot[R1,R2]): Dot[Map[K1,R1],Map[K2,R2]] =
//    new Dot[Map[K1,R1],Map[K2,R2]] {
//      type out = Map[(K1, K2), recur.out]
//
//      def apply(t1: Map[K1, R1], t2: Map[K2, R2]): out = t1.flatMap { case (k1, v1) =>
//        t2.map { case (k2, v2) => (k1, k2) -> recur(v1, v2) }
//      }
//    }
//
//  def dot[T1,T2](x1: T1, x2: T2)(implicit ev: Dot[T1,T2]): ev.out = ev(x1,x2)
//
//  def main(args: Array[String]): Unit = {
//    val map1 = Map(
//      "a" -> 1,
//      "b" -> 2,
//      "c" -> 3
//    )
//    val map2 = Map(
//      "a" -> Map("a" -> 1),
//      "b" -> Map("b" -> 1)
//    )
//    val result = dot(dot(map1,1),1)
//    println(result)
//  }
//}
