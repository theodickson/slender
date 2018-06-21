package slender

import shapeless.ops.hlist.ToTraversable
import shapeless.{::, Generic, HList, HNil, LUBConstraint}

trait Binder[V <: Expr,T,-In,+Out] extends (In => Out) with Serializable


object Binder extends BindingImplicits {
  def nonBinder[V <: Expr, T, E]: Binder[V,T,E,E] =
    new Binder[V,T,E,E] { def apply(v1: E) = v1 }

  def instance[V <: Expr, T, In, Out](f: In => Out): Binder[V,T,In,Out] = new Binder[V,T,In,Out] {
    def apply(v1: In): Out = f(v1)
  }
}

trait Priority0BindingImplicits {
  //The case that V1 <:< V and so actually binds to T takes precedence, hence this non-binder is here.
  implicit def VariableNonBinder[V <: UntypedVariable,V1 <: UntypedVariable,T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]

//  implicit def InductiveBinder[
//  V <: Expr, T, E1 <: Expr,E2 <: Expr, Repr1 <: HList, Repr2 <: HList
//  ]
//  (implicit gen1: Generic.Aux[E1,Repr1], bind: Binder[V, T, Repr1,Repr2],
//   ev: Reconstruct[E1, Repr2, E2], gen2: Generic.Aux[E2, Repr2]):
//  Binder[V, T, E1, E2] = new Binder[V, T, E1, E2] {
//    def apply(v1: E1): E2 = gen2.from(bind(gen1.to(v1)))
//  }
//
//  implicit def HNilBinder[V <: Expr, T]: Binder[V, T, HNil,HNil] =
//    new Binder[V,T,HNil,HNil] {
//      def apply(v1: HNil) = v1
//    }
//
//  implicit def HListBinder[V <: Expr, T, H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
//  (implicit bindH: Binder[V, T, H1, H2], bindT: Binder[V, T, T1, T2]):
//  Binder[V,T,H1 :: T1, H2 :: T2] = new Binder[V,T,H1 :: T1, H2 :: T2] {
//    def apply(v1: H1 :: T1) = v1 match {
//      case h1 :: t1 => bindH(h1) :: bindT(t1)
//    }
//  }

  implicit def PhysicalCollectionBinder[V <: Expr,T,C[_,_],K,R]: Binder[V,T,PhysicalCollection[C,K,R],PhysicalCollection[C,K,R]] =
    Binder.nonBinder[V,T,PhysicalCollection[C,K,R]]

  //The standard (non-transitive) reconstructive methods for tuples take precedence so that there is no ambiguity in the specific
  //case that we request a Binder for a tupled variable expression to apply to a tupled varaible expression, and so either route will work (though
  //both then delegate to the other in symmetry).
  //Also it seems likely that the transitivity puts a lot of strain on the compiler so the lower we push such a search down the tree
  //the easier we make it for the compiler.
//  implicit def BindTuple2[
//  V1 <: Expr, V2 <: Expr, T1, T2, E1 <: Expr, E2 <: Expr, E3 <: Expr
//  ](implicit bind1: Binder[V1, T1, E1, E2], bind2: Binder[V2, T2, E2, E3]): Binder[Tuple2VariableExpr[V1, V2], (T1, T2), E1, E3] =
//    new Binder[Tuple2VariableExpr[V1, V2], (T1, T2), E1, E3] {
//      def apply(v1: E1) = bind2(bind1(v1))
//    }
//
//  implicit def BindTuple3[
//  V1 <: Expr, V2 <: Expr, V3 <: Expr, T1, T2, T3,
//  E1 <: Expr, E2 <: Expr, E3 <: Expr, E4 <: Expr
//  ](implicit bind1: Binder[V1, T1, E1, E2],
//    bind2: Binder[V2, T2, E2, E3],
//    bind3: Binder[V3, T3, E3, E4]): Binder[Tuple3VariableExpr[V1, V2, V3], (T1, T2, T3), E1, E4] =
//    new Binder[Tuple3VariableExpr[V1, V2, V3], (T1, T2, T3), E1, E4] {
//      def apply(v1: E1) = bind3(bind2(bind1(v1)))
//    }

  implicit def BindProduct2[
    V1 <: Expr, V2 <: Expr, T1, T2, E1 <: Expr, E2 <: Expr, E3 <: Expr
  ](implicit bind1: Binder[V1, T1, E1, E2], bind2: Binder[V2, T2, E2, E3]): Binder[ProductExpr[V1 :: V2 :: HNil], (T1, T2), E1, E3] =
    new Binder[ProductExpr[V1::V2::HNil], (T1, T2), E1, E3] {
      def apply(v1: E1) = bind2(bind1(v1))
    }

  implicit def BindProduct3[
  V1 <: Expr, V2 <: Expr, V3 <: Expr, T1, T2, T3,
  E1 <: Expr, E2 <: Expr, E3 <: Expr, E4 <: Expr
  ](implicit bind1: Binder[V1, T1, E1, E2],
    bind2: Binder[V2, T2, E2, E3],
    bind3: Binder[V3, T3, E3, E4]): Binder[ProductExpr[V1 :: V2 :: V3 :: HNil], (T1, T2, T3), E1, E4] =
    new Binder[ProductExpr[V1 :: V2 :: V3 :: HNil], (T1, T2, T3), E1, E4] {
      def apply(v1: E1) = bind3(bind2(bind1(v1)))
    }
}

trait Priority1BindingImplicits extends Priority0BindingImplicits {

  /** Standard inductive binder cases. */
  implicit def AddBinder[V <: Expr, T, L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, AddExpr[L, R], AddExpr[L1, R1]] =
    new Binder[V, T, AddExpr[L, R], AddExpr[L1, R1]] {
      def apply(v1: AddExpr[L, R]) = AddExpr(bindL(v1.c1), bindR(v1.c2))
    }

  implicit def MultiplyBinder[V <: Expr, T, L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, MultiplyExpr[L, R], MultiplyExpr[L1, R1]] =
    new Binder[V, T, MultiplyExpr[L, R], MultiplyExpr[L1, R1]] {
      def apply(v1: MultiplyExpr[L, R]) = MultiplyExpr(bindL(v1.c1), bindR(v1.c2))
    }

  implicit def DotBinder[V <: Expr, T, L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, DotExpr[L, R], DotExpr[L1, R1]] =
    new Binder[V, T, DotExpr[L, R], DotExpr[L1, R1]] {
      def apply(v1: DotExpr[L, R]) = DotExpr(bindL(v1.c1), bindR(v1.c2))
    }

  implicit def JoinBinder[V <: Expr, T, L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, JoinExpr[L, R], JoinExpr[L1, R1]] =
    new Binder[V, T, JoinExpr[L, R], JoinExpr[L1, R1]] {
      def apply(v1: JoinExpr[L, R]) = JoinExpr(bindL(v1.c1), bindR(v1.c2))
    }

  //This applies a single binder to a Tuple2 variable expression.

  implicit def NotBinder[V <: Expr, T, R <: Expr, R1 <: Expr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, NotExpr[R], NotExpr[R1]] =
    new Binder[V, T, NotExpr[R], NotExpr[R1]] {
      def apply(v1: NotExpr[R]) = NotExpr(recur(v1.c1))
    }

  implicit def NegateBinder[V <: Expr, T, R <: Expr, R1 <: Expr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, NegateExpr[R], NegateExpr[R1]] =
    new Binder[V, T, NegateExpr[R], NegateExpr[R1]] {
      def apply(v1: NegateExpr[R]) = NegateExpr(recur(v1.c1))
    }

  implicit def SumBinder[V <: Expr, T, R <: Expr, R1 <: Expr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, SumExpr[R], SumExpr[R1]] =
    new Binder[V, T, SumExpr[R], SumExpr[R1]] {
      def apply(v1: SumExpr[R]) = SumExpr(recur(v1.c1))
    }

  implicit def GroupBinder[V <: Expr, T, R <: Expr, R1 <: Expr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, GroupExpr[R], GroupExpr[R1]] =
    new Binder[V, T, GroupExpr[R], GroupExpr[R1]] {
      def apply(v1: GroupExpr[R]) = GroupExpr(recur(v1.c1))
    }

  implicit def SngBinder[V <: Expr, T, K <: Expr, R <: Expr, K1 <: Expr, R1 <: Expr]
  (implicit bindK: Binder[V, T, K, K1], bindR: Binder[V, T, R, R1]): Binder[V, T, SngExpr[K, R], SngExpr[K1, R1]] =
    new Binder[V, T, SngExpr[K, R], SngExpr[K1, R1]] {
      def apply(v1: SngExpr[K, R]) = SngExpr(bindK(v1.key), bindR(v1.value))
    }

  implicit def EqualsPredicateBinder[V <: Expr, T, K1 <: Expr, K2 <: Expr, K1B <: Expr, K2B <: Expr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B]): Binder[V, T, EqualsPredicate[K1, K2], EqualsPredicate[K1B, K2B]] =
    new Binder[V, T, EqualsPredicate[K1, K2], EqualsPredicate[K1B, K2B]] {
      def apply(v1: EqualsPredicate[K1, K2]) = EqualsPredicate(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def IntPredicateBinder[V <: Expr, T, K1 <: Expr, K2 <: Expr, K1B <: Expr, K2B <: Expr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B]): Binder[V, T, IntPredicate[K1, K2], IntPredicate[K1B, K2B]] =
    new Binder[V, T, IntPredicate[K1, K2], IntPredicate[K1B, K2B]] {
      def apply(v1: IntPredicate[K1, K2]) = IntPredicate(bind1(v1.c1), bind2(v1.c2), v1.p, v1.opString)
    }

  implicit def ProductBinder[V <: Expr,T,C <: HList, CR <: HList]
  (implicit resolve: Binder[V,T,C,CR], lub: LUBConstraint[CR,Expr]): Binder[V,T,ProductExpr[C],ProductExpr[CR]] =
    new Binder[V,T,ProductExpr[C],ProductExpr[CR]] {
      def apply(v1: ProductExpr[C]) = ProductExpr(resolve(v1.exprs))
    }

  implicit def HNilBinder[V <: Expr,T]: Binder[V,T,HNil,HNil] = Binder.nonBinder[V,T,HNil]

  implicit def HListBinder[V <: Expr, T, H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
  (implicit resolveH: Binder[V,T,H1,H2], resolveT: Binder[V,T,T1,T2]):
  Binder[V,T,H1 :: T1, H2 :: T2] = new Binder[V,T,H1 :: T1, H2 :: T2] {
    def apply(v1: H1 :: T1): H2 :: T2 = v1 match {
      case h1 :: t1 => resolveH(h1) :: resolveT(t1)
    }
  }
}

trait BindingImplicits extends Priority1BindingImplicits {
  //Never try to find an actual binder for a typed variable.
  implicit def TypedNonBinder[V <: Expr, T, T1]: Binder[V, T, TypedVariable[T1], TypedVariable[T1]] = Binder.nonBinder[V, T, TypedVariable[T1]]

  implicit def UnusedBinder1[V <: Expr, T]: Binder[V, T, UnusedVariable, UnusedVariable] = Binder.nonBinder[V, T, UnusedVariable]

  implicit def UnusedBinder2[V <: Expr, T]: Binder[UnusedVariable, T, V, V] = Binder.nonBinder[UnusedVariable, T, V]

  /** Binding base cases - primitive expressions don't need to bind anything, variables bind iff they are untyped variables
    * matching the type in the binder. (otherwise they use the low priority non-binding case)
    */
  implicit def PrimitiveKeyExprBinder[V <: Expr, T, V1]: Binder[V, T, PrimitiveKeyExpr[V1], PrimitiveKeyExpr[V1]] =
    Binder.nonBinder[V, T, PrimitiveKeyExpr[V1]]

  implicit def NumericExprBinder[V <: Expr, T, V1]: Binder[V, T, NumericExpr[V1],NumericExpr[V1]] =
    Binder.nonBinder[V, T, NumericExpr[V1]]

  implicit def VariableBinder[V <: UntypedVariable, T]: Binder[V, T, V, TypedVariable[T]] =
    new Binder[V, T, V, TypedVariable[T]] {
      def apply(v1: V): TypedVariable[T] = v1.tag[T]
    }

  /** Special binder for inner infinite mappings - dont attempt to bind the key */
  implicit def InfMappingBinder[V <: Expr, T, K <: Expr, R <: Expr, R1 <: Expr]
  (implicit bindR: Binder[V, T, R, R1]): Binder[V, T, InfiniteMappingExpr[K, R], InfiniteMappingExpr[K, R1]] =
    new Binder[V, T, InfiniteMappingExpr[K, R], InfiniteMappingExpr[K, R1]] {
      def apply(v1: InfiniteMappingExpr[K, R]) = InfiniteMappingExpr(v1.key, bindR(v1.value))
    }
}
