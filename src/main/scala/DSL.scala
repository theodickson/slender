package slender

trait DSL {

//  implicit class RingExprOps[R <: RingExpr](expr: R) {
//
//    def +[R1 <: RingExpr](expr1: R1) = AddExpr(expr, expr1)
//
//    def *[R1 <: RingExpr](expr1: R1) = MultiplyExpr(expr, expr1)
//
//    def dot[R1 <: RingExpr](expr1: R1) = DotExpr(expr, expr1)
//
//    def sum = SumExpr(expr)
//
//    def unary_- = NegateExpr(expr)
//
//    def unary_! = NotExpr(expr)
//
//    //    def _1: RingExpr = ProjectRingExpr(e, 1)
//    //    def _2: RingExpr = ProjectRingExpr(e, 2)
//    //
//
//  }

//  implicit class KeyExprOps[K <: KeyExpr](val k: K) {
//    //      def _1: KeyExpr = ProjectKeyExpr(k, 1)
//    //      def _2: KeyExpr = ProjectKeyExpr(k, 2)
//    //      def _3: KeyExpr = ProjectKeyExpr(k, 3)
//
//    def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(k, k1)
//
//    def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(k, k1))
//
//    def >[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ > _, ">")
//
//    def <[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ < _, "<")
//
//    def -->[R <: RingExpr](r: R): (K, R) = (k, r)
//  }

//
//  implicit class VariableExprOps[V <: VariableExpr](val v: V) {
//      def <--[R <: RingExpr](r: R): (V, R) = (v,r)
//      def ==>[R <: RingExpr,T](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(v,r)
//  }
//  trait MakeRing
  trait MakeExpr[X,E <: Expr] extends (X => E)

  implicit def toExpr[X,E <: Expr](x: X)(implicit make: MakeExpr[X,E]): E = make(x)

  implicit def idMakeExpr[E <: Expr]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }

  implicit def tuple2MakeRingExpr[X1,X2,R1 <: RingExpr, R2 <: RingExpr]
    (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2RingExpr(recur1(v1._1),recur2(v1._2))
    }

  implicit def tuple3MakeRingExpr[X1,X2,X3,R1 <: RingExpr, R2 <: RingExpr, R3 <: RingExpr]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
  MakeExpr[(X1,X2,X3),Tuple3RingExpr[R1,R2,R3]] =
    new MakeExpr[(X1,X2,X3),Tuple3RingExpr[R1,R2,R3]] {
      def apply(v1: (X1,X2,X3)) = Tuple3RingExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
    }

  implicit def tuple2MakeVariableExpr[X1,X2,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2VariableExpr(recur1(v1._1),recur2(v1._2))
    }

  implicit def tuple3MakeVariableExpr[X1,X2,X3,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2], R3 <: VariableExpr[R3]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
    MakeExpr[(X1,X2,X3),Tuple3VariableExpr[R1,R2,R3]] =
    new MakeExpr[(X1,X2,X3),Tuple3VariableExpr[R1,R2,R3]] {
      def apply(v1: (X1,X2,X3)) = Tuple3VariableExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
    }


  implicit def sng[K <: KeyExpr, R <: RingExpr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  implicit def sng[K <: KeyExpr](k: K): SngExpr[K, IntExpr] = SngExpr(k, IntExpr(1))

  def toK[E <: RingExpr](e: E): BoxedRingExpr[E] = BoxedRingExpr(e)

//  def fromK[E <: RingExpr](e: BoxedRingExpr[E]): E = e.c1

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)

  case class ForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr](x: V, r1: R) {

    def Collect[R2 <: RingExpr, Out <: Expr]
      (r2: R2)
      (implicit resolver: Resolver[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]], Out]) =
        resolver(SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, r2))))

    def Yield[K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
      (pair: (K, R2))
      (implicit resolver: Resolver[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, R2]]]], Out]): Out =
        Collect(sng(pair._1, pair._2))

    def Yield[K <: KeyExpr, Out <: Expr]
      (k: K)
      (implicit resolver: Resolver[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, IntExpr]]]], Out]): Out =
        Yield(k, IntExpr(1))

  }

  object For {
    def apply[V <: VariableExpr[V], R <: RingExpr](paired: (V, R)): ForComprehensionBuilder[V, R] =
      ForComprehensionBuilder[V, R](paired._1, paired._2)
  }
//
//  implicit class IffImplicit[V <: VariableExpr[V], R <: RingExpr](pair: (V,R)) {
//    def iff[R1 <: RingExpr](r1: R1) = (pair._1,pair._2 dot r1)
//  }

}


////
////Variabl
////

////
