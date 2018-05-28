package slender

//todo with external evaluation, there is scope to put variables into the type system and then possibly
//remove the need to tag them
trait EvalImplicits {

  implicit def VariableEval[V <: UntypedVariable[V],T]: Eval[TypedVariable[T],T] = new Eval[TypedVariable[T],T] {
    def apply(v1: TypedVariable[T], v2: BoundVars): T = v2(v1.name).asInstanceOf[T]
  }

  implicit def Tuple2VariableEval[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2],T1,T2]
  (implicit eval1: Eval[V1,T1], eval2: Eval[V2,T2]): Eval[Tuple2VariableExpr[V1,V2],(T1,T2)] =
    new Eval[Tuple2VariableExpr[V1,V2],(T1,T2)] {
      def apply(v1: Tuple2VariableExpr[V1,V2], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def Tuple3VariableEval[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2],V3 <: VariableExpr[V3],T1,T2,T3]
  (implicit eval1: Eval[V1,T1], eval2: Eval[V2,T2], eval3: Eval[V3,T3]): Eval[Tuple3VariableExpr[V1,V2,V3],(T1,T2,T3)] =
    new Eval[Tuple3VariableExpr[V1,V2,V3],(T1,T2,T3)] {
      def apply(v1: Tuple3VariableExpr[V1,V2,V3], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2),eval3(v1.c3,v2))
    }


  implicit def PrimitiveExprEval[E <: Expr,V](implicit ev: E <:< PrimitiveExpr[V]): Eval[E,V] = new Eval[E,V] {
    def apply(v1: E, v2: BoundVars): V = v1.value
  }

  implicit def InfiniteMappingEval[V <: VariableExpr[V] { type Type = KT },R <: RingExpr,KT,RT]
  (implicit evalK: Eval[V,KT], evalR: Eval[R,RT]): Eval[InfiniteMappingExpr[V,R],KT => RT] =
    new Eval[InfiniteMappingExpr[V,R],KT => RT] {
      //todo
      def apply(v1: InfiniteMappingExpr[V,R], v2: BoundVars): KT => RT = (k: KT) => evalR(v1.value,v2 ++ v1.key.bind(k))//.asInstanceOf[v1.key.Type]))
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

  implicit def NotEval[E <: RingExpr,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NotExpr[E],T] = new Eval[NotExpr[E],T] {
    def apply(v1: NotExpr[E], v2: BoundVars): T = ring.not(eval(v1.c1,v2))
  }

  implicit def NegateEval[E <: RingExpr,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NegateExpr[E],T] = new Eval[NegateExpr[E],T] {
    def apply(v1: NegateExpr[E], v2: BoundVars): T = ring.negate(eval(v1.c1,v2))
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

  implicit def EqualsPredicateEval[K1 <: KeyExpr,K2 <: KeyExpr,T](implicit eval1: Eval[K1,T], eval2: Eval[K2,T]) =
    new Eval[EqualsPredicate[K1,K2],Int] {
      def apply(v1: EqualsPredicate[K1,K2], v2: BoundVars): Int = if (eval1(v1.c1,v2) == eval2(v1.c2,v2)) 1 else 0
    }

  implicit def IntPredicateEval[K1 <: KeyExpr,K2 <: KeyExpr](implicit eval1: Eval[K1,Int], eval2: Eval[K2,Int]) =
    new Eval[IntPredicate[K1,K2],Int] {
      def apply(v1: IntPredicate[K1,K2], v2: BoundVars): Int = if (v1.p(eval1(v1.c1,v2),eval2(v1.c2,v2))) 1 else 0
    }

  implicit def Tuple2KeyEval[K1 <: KeyExpr, K2 <: KeyExpr,T1,T2](implicit eval1: Eval[K1,T1], eval2: Eval[K2,T2]) =
    new Eval[Tuple2KeyExpr[K1,K2],(T1,T2)] {
      def apply(v1: Tuple2KeyExpr[K1,K2], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def Tuple3KeyEval[K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr,T1,T2,T3]
  (implicit eval1: Eval[K1,T1], eval2: Eval[K2,T2], eval3: Eval[K3,T3]) =
    new Eval[Tuple3KeyExpr[K1,K2,K3],(T1,T2,T3)] {
      def apply(v1: Tuple3KeyExpr[K1,K2,K3], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2),eval3(v1.c3,v2))
    }

  implicit def Tuple2RingEval[K1 <: RingExpr, K2 <: RingExpr,T1,T2](implicit eval1: Eval[K1,T1], eval2: Eval[K2,T2]) =
    new Eval[Tuple2RingExpr[K1,K2],(T1,T2)] {
      def apply(v1: Tuple2RingExpr[K1,K2], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def Tuple3RingEval[K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr,T1,T2,T3]
  (implicit eval1: Eval[K1,T1], eval2: Eval[K2,T2], eval3: Eval[K3,T3]) =
    new Eval[Tuple3RingExpr[K1,K2,K3],(T1,T2,T3)] {
      def apply(v1: Tuple3RingExpr[K1,K2,K3], v2: BoundVars) = (eval1(v1.c1,v2),eval2(v1.c2,v2),eval3(v1.c3,v2))
    }

  implicit def Project2_1KeyEval[K <: KeyExpr with C1Expr,KT1](implicit eval: Eval[K,(KT1,_)]) =
    new Eval[Project1KeyExpr[K],KT1] { def apply(v1: Project1KeyExpr[K], v2: BoundVars) = eval(v1.c1,v2)._1 }

  implicit def Project2_2KeyEval[K <: KeyExpr with C2Expr,KT2](implicit eval: Eval[K,(_,KT2)]) =
    new Eval[Project2KeyExpr[K],KT2] { def apply(v1: Project2KeyExpr[K], v2: BoundVars) = eval(v1.c1,v2)._2 }

  implicit def Project3_1KeyEval[K <: KeyExpr with C1Expr,KT1](implicit eval: Eval[K,(KT1,_,_)]) =
    new Eval[Project1KeyExpr[K],KT1] { def apply(v1: Project1KeyExpr[K], v2: BoundVars) = eval(v1.c1,v2)._1 }

  implicit def Project3_2KeyEval[K <: KeyExpr with C2Expr,KT2](implicit eval: Eval[K,(_,KT2,_)]) =
    new Eval[Project2KeyExpr[K],KT2] { def apply(v1: Project2KeyExpr[K], v2: BoundVars) = eval(v1.c1,v2)._2 }

  implicit def Project3_3KeyEval[K <: KeyExpr with C3Expr,KT3](implicit eval: Eval[K,(_,_,KT3)]) =
    new Eval[Project3KeyExpr[K],KT3] { def apply(v1: Project3KeyExpr[K], v2: BoundVars) = eval(v1.c1,v2)._3 }

  implicit def Project2_1RingEval[K <: RingExpr with C1Expr,KT1](implicit eval: Eval[K,(KT1,_)]) =
    new Eval[Project1RingExpr[K],KT1] { def apply(v1: Project1RingExpr[K], v2: BoundVars) = eval(v1.c1,v2)._1 }

  implicit def Project2_2RingEval[K <: RingExpr with C2Expr,KT2](implicit eval: Eval[K,(_,KT2)]) =
    new Eval[Project2RingExpr[K],KT2] { def apply(v1: Project2RingExpr[K], v2: BoundVars) = eval(v1.c1,v2)._2 }

  implicit def Project3_1RingEval[K <: RingExpr with C1Expr,KT1](implicit eval: Eval[K,(KT1,_,_)]) =
    new Eval[Project1RingExpr[K],KT1] { def apply(v1: Project1RingExpr[K], v2: BoundVars) = eval(v1.c1,v2)._1 }

  implicit def Project3_2RingEval[K <: RingExpr with C2Expr,KT2](implicit eval: Eval[K,(_,KT2,_)]) =
    new Eval[Project2RingExpr[K],KT2] { def apply(v1: Project2RingExpr[K], v2: BoundVars) = eval(v1.c1,v2)._2 }

  implicit def Project3_3RingEval[K <: RingExpr with C3Expr,KT3](implicit eval: Eval[K,(_,_,KT3)]) =
    new Eval[Project3RingExpr[K],KT3] { def apply(v1: Project3RingExpr[K], v2: BoundVars) = eval(v1.c1,v2)._3 }


}
