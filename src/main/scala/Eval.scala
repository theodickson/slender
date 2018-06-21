package slender

import shapeless.ops.hlist.Tupler
import shapeless.ops.tuple.At
import shapeless.{::, Generic, HList, HNil, Nat, Select}
import shapeless.syntax.HListOps

trait Eval[-E,+T] extends ((E,BoundVars) => T) with Serializable

object Eval {

  def instance[E,T](f: (E,BoundVars) => T): Eval[E,T] = new Eval[E,T] {
    def apply(v1: E, v2: BoundVars): T = f(v1,v2)
  }

  implicit def VariableEval[T]: Eval[TypedVariable[T],T] = new Eval[TypedVariable[T],T] {
    def apply(v1: TypedVariable[T], v2: BoundVars): T = v2(v1.name).asInstanceOf[T]
  }

  implicit def UnusedVariableEval: Eval[UnusedVariable,Any] = new Eval[UnusedVariable,Any] {
    def apply(v1: UnusedVariable, v2: BoundVars): Any = null
  }

  implicit def PrimitiveKeyExprEval[V]: Eval[PrimitiveKeyExpr[V],V] = new Eval[PrimitiveKeyExpr[V],V] {
    def apply(v1: PrimitiveKeyExpr[V], v2: BoundVars): V = v1.value
  }

  implicit def NumericExprEval[V]: Eval[NumericExpr[V],V] = new Eval[NumericExpr[V],V] {
    def apply(v1: NumericExpr[V], v2: BoundVars): V = v1.value
  }

  implicit def PhysicalCollectionEval[C[_,_],K,R]: Eval[PhysicalCollection[C,K,R],C[K,R]] =
    new Eval[PhysicalCollection[C, K, R],C[K,R]] {
      def apply(v1: PhysicalCollection[C,K,R], v2: BoundVars): C[K,R] = v1.value
  }

  implicit def InfiniteMappingEval[V <: Expr,R <: Expr,KT,RT]
  (implicit evalK: Eval[V,KT], evalR: Eval[R,RT], varBinder: VarBinder[V, KT]): Eval[InfiniteMappingExpr[V,R],KT => RT] =
    new Eval[InfiniteMappingExpr[V,R],KT => RT] {
      def apply(v1: InfiniteMappingExpr[V,R], v2: BoundVars): KT => RT =
        (k: KT) => evalR(v1.value,v2 ++ varBinder(v1.key, k))
    }


  implicit def MultiplyEval[E1 <: Expr,E2 <: Expr,T1,T2,O](implicit eval1: Eval[E1,T1],
                                                                   eval2: Eval[E2,T2],
                                                                   mult: Multiply[T1,T2,O]): Eval[MultiplyExpr[E1,E2],O] =
    new Eval[MultiplyExpr[E1,E2],O] {
      def apply(v1: MultiplyExpr[E1,E2], v2: BoundVars): O = mult(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def DotEval[E1 <: Expr,E2 <: Expr,T1,T2,O](implicit eval1: Eval[E1,T1],
                                                              eval2: Eval[E2,T2],
                                                              dot: Dot[T1,T2,O]): Eval[DotExpr[E1,E2],O] =
    new Eval[DotExpr[E1,E2],O] {
      def apply(v1: DotExpr[E1,E2], v2: BoundVars): O = dot(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def JoinEval[E1 <: Expr,E2 <: Expr,T1,T2,O](implicit eval1: Eval[E1,T1],
                                                              eval2: Eval[E2,T2],
                                                               join: Join[T1,T2,O]): Eval[JoinExpr[E1,E2],O] =
    new Eval[JoinExpr[E1,E2],O] {
      def apply(v1: JoinExpr[E1,E2], v2: BoundVars): O = join(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def AddEval[E1 <: Expr,E2 <: Expr,T](implicit eval1: Eval[E1,T],
                                                        eval2: Eval[E2,T],
                                                        ring: Ring[T]): Eval[AddExpr[E1,E2],T] =
    new Eval[AddExpr[E1,E2],T] {
      def apply(v1: AddExpr[E1,E2], v2: BoundVars): T = ring.add(eval1(v1.c1,v2),eval2(v1.c2,v2))
    }

  implicit def SumEval[E <: Expr,C[_,_],K,R,O](implicit eval: Eval[E,C[K,R]], sum: Sum[C[K,R],O]):
  Eval[SumExpr[E],O] = new Eval[SumExpr[E],O] {
    def apply(v1: SumExpr[E], v2: BoundVars): O = sum(eval(v1.c1,v2))
  }

  implicit def GroupEval[E <: Expr,C[_,_],K,R,O](implicit eval: Eval[E,C[K,R]], group: Group[C[K,R],O]):
  Eval[GroupExpr[E],O] = new Eval[GroupExpr[E],O] {
    def apply(v1: GroupExpr[E], v2: BoundVars): O = group(eval(v1.c1,v2))
  }

  implicit def NotEval[E <: Expr,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NotExpr[E],T] = new Eval[NotExpr[E],T] {
    def apply(v1: NotExpr[E], v2: BoundVars): T = ring.not(eval(v1.c1,v2))
  }

  implicit def NegateEval[E <: Expr,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NegateExpr[E],T] = new Eval[NegateExpr[E],T] {
    def apply(v1: NegateExpr[E], v2: BoundVars): T = ring.negate(eval(v1.c1,v2))
  }

  implicit def SngEval[K <: Expr,R <: Expr,TK,TR](implicit evalK: Eval[K,TK], evalR: Eval[R,TR]):
  Eval[SngExpr[K,R],Map[TK,TR]] = new Eval[SngExpr[K,R],Map[TK,TR]] {
    def apply(v1: SngExpr[K,R], v2: BoundVars): Map[TK,TR] = Map(evalK(v1.key,v2) -> evalR(v1.value,v2))
  }

  implicit def EqualsPredicateEval[K1 <: Expr,K2 <: Expr,T](implicit eval1: Eval[K1,T], eval2: Eval[K2,T]) =
    new Eval[EqualsPredicate[K1,K2],Int] {
      def apply(v1: EqualsPredicate[K1,K2], v2: BoundVars): Int = if (eval1(v1.c1,v2) == eval2(v1.c2,v2)) 1 else 0
    }

  implicit def IntPredicateEval[K1 <: Expr,K2 <: Expr](implicit eval1: Eval[K1,Int], eval2: Eval[K2,Int]) =
    new Eval[IntPredicate[K1,K2],Int] {
      def apply(v1: IntPredicate[K1,K2], v2: BoundVars): Int = if (v1.p(eval1(v1.c1,v2),eval2(v1.c2,v2))) 1 else 0
    }

  implicit def ProductEval[Exprs <: HList,O <: HList,T](implicit eval: Eval[Exprs,O], tupler: Tupler.Aux[O,T]):
    Eval[ProductExpr[Exprs], T] =
      instance { case (ProductExpr(exprs),bvs) => tupler(eval(exprs,bvs)) }

  implicit def HNilEval: Eval[HNil,HNil] = instance[HNil,HNil] { case (HNil,_) => HNil }

  implicit def HConsEval[H <: Expr,T <: HList,HO,TO <: HList]
    (implicit evalH: Eval[H,HO], evalT: Eval[T,TO]): Eval[H :: T, HO :: TO] = instance[H::T,HO::TO] {
      case (h :: t,bvs) => evalH(h, bvs) :: evalT(t, bvs)
    }

}


