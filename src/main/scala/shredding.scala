package slender

object shredding {

//  def shred[T,E <: Expr[T],TS,ES <: Expr[TS]](expr: E)(implicit shredder: Shredder[T,E,TS,ES]): ES = shredder(expr)

  trait Shredder[T <: Expr[T], S <: Expr[S]] extends (T => S)

  trait NonShredder[T <: Expr[T]] extends Shredder[T,T] {
    def apply(v1: T): T = v1
  }

  implicit def VariableShredder[K]: NonShredder[Variable[K]] = new NonShredder[Variable[K]] { }

  implicit def PhysicalCollectionShredder[C[_,_],K,R]: NonShredder[PhysicalCollection[C,K,R]] =
    new NonShredder[PhysicalCollection[C,K,R]] {}

  implicit def SelfDotShredder[E <: Expr[E],ES <: Expr[ES]](implicit recur: Shredder[E,ES]): Shredder[SelfDotExpr[E],SelfDotExpr[ES]] =
    new Shredder[SelfDotExpr[E],SelfDotExpr[ES]] {
      def apply(v1: SelfDotExpr[E]): SelfDotExpr[ES] = SelfDotExpr(recur(v1.c1),recur(v1.c2))
    }
//
//  implicit def DotShredder[T1,T1S,T2,T2S,E1[X]<:Expr[X],E1S[X] <: Expr[X],E2[X] <: Expr[X],E2S[X] <: Expr[X],O,OS](
//                                                              implicit recur1: Shredder[T1,T1S,E1[T1],E1S[T1S]],
//                                                              recur2: Shredder[T2,T2S,E2[T2],E2S[T2S]],
//                                                              dot: Dot[T1S,T2S,OS]
//                                                            ): Shredder[O,OS,DotExpr[T1,T2,E1,E2,O],DotExpr[T1S,T2S,E1S,E2S,OS]] =
//    new Shredder[O,OS,DotExpr[T1,T2,E1,E2,O],DotExpr[T1S,T2S,E1S,E2S,OS]] {
//      def apply(t: DotExpr[T1,T2,E1,E2,O]): DotExpr[T1S,T2S,E1S,E2S,OS] = DotExpr[T1S,T2S,E1S,E2S,OS](recur1(t.c1),recur2(t.c2))
//    }

//  implicit def BoxedRingShredder[O,OS, E <: Expr[O],ES <: Expr[ES]](implicit recur: Shredder[O,OS,E,ES]):
//    Shredder[O,OS,BoxedRingExpr[O,E],LabelExpr[OS,ES]] = new Shredder[O,OS,BoxedRingExpr[O,E],LabelExpr[OS,ES]] {
//      def apply(boxed: BoxedRingExpr[O,E]): LabelExpr[OS,ES] = LabelExpr(boxed.c1.shred)
//    }

}