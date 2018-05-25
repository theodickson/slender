package slender

//todo with external evaluation, there is scope to put variables into the type system and then possibly
//remove the need to tag them
trait EvalImplicits {

  implicit def VariableEval[T]: Eval[Variable[T],T] = new Eval[Variable[T],T] {
    def apply(v1: Variable[T],v2: BoundVars): T = v2(v1).asInstanceOf[T]
  }

//  implicit def Tuple2VariableEval[V1 <: VariableExpr[_],V2 <: VariableExpr[_],T1,T2]
//  (implicit ev1: V1 <:< VariableExpr[T1], ev2: V2 <:< VariableExpr[T2],
//   eval1: Eval[V1,T1], eval2: Eval[V2,T2]): Eval[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2)] =
//    new Eval[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2)] {
//      def apply(v1: Tuple2VariableExpr[V1,V2,T1,T2], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2))
//    }

//  implicit def Tuple2VariableEval[V1 <: VariableExpr[_],V2 <: VariableExpr[_],T1,T2]
//  (implicit eval1: Eval[V1,T1],
//            eval2: Eval[V2,T2],
//            ev1: V1 <:< VariableExpr[T1],
//            ev2: V2 <:< VariableExpr[T2]): Eval[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2)] =
//
//    new Eval[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2)] {
//      def apply(v1: Tuple2VariableExpr[V1,V2,T1,T2], v2: BoundVars): (T1,T2) = {
//        (eval1(v1.c1,v2),eval2(v1.c2,v2))
////        val casted = v1.asInstanceOf[Tuple2VariableExpr[_,_,T1,T2]]
////        val _1 = eval1(casted.c1.asInstanceOf[VariableExpr[T1]],v2)
////        val _2 = eval2(casted.c2.asInstanceOf[VariableExpr[T2]],v2)
////        (_1,_2)
//      }
//    }

  implicit def PrimitiveExprEval[E <: Expr,V](implicit ev: E <:< PrimitiveExpr[V]): Eval[E,V] = new Eval[E,V] {
    def apply(v1: E, v2: BoundVars): V = v1.value
  }

//  implicit def InfiniteMappingEval[K,R <: RingExpr,T](implicit evalK: Eval[Variable[K],K], evalR: Eval[R,T]):
//  Eval[InfiniteMappingExpr[K,R],K => T] = new Eval[InfiniteMappingExpr[K,R],K => T] {
//    def apply(v1: InfiniteMappingExpr[K,R], v2: BoundVars): K => T = (k: K) => evalR(v1.value,v2 + (v1.key -> k))
//  }

  implicit def InfiniteMappingEval[K,R <: RingExpr,RT]
  (implicit evalK: Eval[VariableExpr[K],K], evalR: Eval[R,RT]): Eval[InfiniteMappingExpr[K,R],K => RT] =
    new Eval[InfiniteMappingExpr[K,R],K => RT] {
      def apply(v1: InfiniteMappingExpr[K,R], v2: BoundVars): K => RT = (k: K) => evalR(v1.value,v2 ++ v1.key.bind(k))
  }


  implicit def MultiplyEval[E1 <: RingExpr,E2 <: RingExpr,T1,T2,O](implicit eval1: Eval[E1,T1],
                                                                   eval2: Eval[E2,T2],
                                                                   mult: Multiply[T1,T2,O]): Eval[MultiplyExpr[E1,E2],O] =
    new Eval[MultiplyExpr[E1,E2],O] {
      def apply(v1: MultiplyExpr[E1,E2], v2: BoundVars): O = mult(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def DotEval[E1 <: RingExpr,E2 <: RingExpr,T1,T2,O](implicit eval1: Eval[E1,T1],
                                                              eval2: Eval[E2,T2],
                                                              dot: Dot[T1,T2,O]): Eval[DotExpr[E1,E2],O] =
    new Eval[DotExpr[E1,E2],O] {
      def apply(v1: DotExpr[E1,E2], v2: BoundVars): O = dot(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def AddEval[E1 <: RingExpr,E2 <: RingExpr,T](implicit eval1: Eval[E1,T],
                                                        eval2: Eval[E2,T],
                                                        ring: Ring[T]): Eval[AddExpr[E1,E2],T] =
    new Eval[AddExpr[E1,E2],T] {
      def apply(v1: AddExpr[E1,E2], v2: BoundVars): T = ring.add(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def SumEval[E <: RingExpr,C[_,_],K,R,O](implicit eval: Eval[E,C[K,R]], coll: Collection[C,K,R]):
  Eval[SumExpr[E],R] = new Eval[SumExpr[E],R] {
    def apply(v1: SumExpr[E], v2: BoundVars): R = coll.sum(eval(v1.c1,v2))
  }

  implicit def SngEval[K <: KeyExpr,R <: RingExpr,TK,TR](implicit evalK: Eval[K,TK], evalR: Eval[R,TR]):
  Eval[SngExpr[K,R],Map[TK,TR]] = new Eval[SngExpr[K,R],Map[TK,TR]] {
    def apply(v1: SngExpr[K,R], v2: BoundVars): Map[TK,TR] = Map(evalK(v1.key,v2) -> evalR(v1.value,v2))
  }

  implicit def BoxedRingEval[R <: RingExpr,T](implicit eval: Eval[R,T]): Eval[BoxedRingExpr[R],T] =
    new Eval[BoxedRingExpr[R],T] {
      def apply(v1: BoxedRingExpr[R],v2: BoundVars): T = eval(v1.c1,v2)
    }

  implicit def LabelEval[R <: RingExpr,T](implicit eval: Eval[R,T]): Eval[LabelExpr[R],Label[R,T]] =
    new Eval[LabelExpr[R],Label[R,T]] {
      //todo = filter vars to free variables actually in v1.c1
      override def apply(v1: LabelExpr[R], v2: BoundVars): Label[R, T] = Label[R,T](v1.c1,v2,eval)
    }

  implicit def Tuple2KeyEval[K1 <: KeyExpr, K2 <: KeyExpr,T1,T2](implicit eval1: Eval[K1,T1], eval2: Eval[K2,T2]) =
    new Eval[Tuple2KeyExpr[K1,K2],(T1,T2)] {
      def apply(v1: Tuple2KeyExpr[K1,K2], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2))
    }


}
