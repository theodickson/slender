package slender

import shapeless.{::, Generic, HList, HNil}

trait Binder[V <: VariableExpr[V],T,-In,+Out] extends (In => Out) with Serializable

//trait HBinder[V <: VariableExpr[V],T,-In,+Out] extends (In => Out) with Serializable

object Binder extends BindingImplicits {
  def nonBinder[V <: VariableExpr[V], T, E <: Expr]: Binder[V,T,E,E] =
    new Binder[V,T,E,E] { def apply(v1: E) = v1 }

  def instance[V <: VariableExpr[V], T, In, Out](f: In => Out): Binder[V,T,In,Out] = new Binder[V,T,In,Out] {
    def apply(v1: In): Out = f(v1)
  }
}

//object HBinder {
//  implicit def HNilBinder[V <: VariableExpr[V], T]: HBinder[V, T, HNil,HNil] =
//    new HBinder[V,T,HNil,HNil] {
//      def apply(v1: HNil) = v1
//    }
//
//  implicit def HListBinder[V <: VariableExpr[V], T, H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
//  (implicit bindH: Binder[V, T, H1, H2], bindT: HBinder[V, T, T1, T2]):
//  HBinder[V,T,H1 :: T1, H2 :: T2] = new HBinder[V,T,H1 :: T1, H2 :: T2] {
//    def apply(v1: H1 :: T1) = v1 match {
//      case h1 :: t1 => bindH(h1) :: bindT(t1)
//    }
//  }
//}

trait Priority0BindingImplicits {
  //The case that V1 <:< V and so actually binds to T takes precedence, hence this non-binder is here.
  implicit def VariableNonBinder[V <: UntypedVariable[V],V1 <: UntypedVariable[V1],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]

//  implicit def InductiveBinder[
//  V <: VariableExpr[V], T, E1 <: Expr,E2 <: Expr, Repr1 <: HList, Repr2 <: HList
//  ]
//  (implicit gen1: Generic.Aux[E1,Repr1], bind: Binder[V, T, Repr1,Repr2],
//   ev: Reconstruct[E1, Repr2, E2], gen2: Generic.Aux[E2, Repr2]):
//  Binder[V, T, E1, E2] = new Binder[V, T, E1, E2] {
//    def apply(v1: E1): E2 = gen2.from(bind(gen1.to(v1)))
//  }
//
//  implicit def HNilBinder[V <: VariableExpr[V], T]: Binder[V, T, HNil,HNil] =
//    new Binder[V,T,HNil,HNil] {
//      def apply(v1: HNil) = v1
//    }
//
//  implicit def HListBinder[V <: VariableExpr[V], T, H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
//  (implicit bindH: Binder[V, T, H1, H2], bindT: Binder[V, T, T1, T2]):
//  Binder[V,T,H1 :: T1, H2 :: T2] = new Binder[V,T,H1 :: T1, H2 :: T2] {
//    def apply(v1: H1 :: T1) = v1 match {
//      case h1 :: t1 => bindH(h1) :: bindT(t1)
//    }
//  }

  implicit def PhysicalCollectionBinder[V <: VariableExpr[V],T,C[_,_],K,R]: Binder[V,T,PhysicalCollection[C,K,R],PhysicalCollection[C,K,R]] =
    Binder.nonBinder[V,T,PhysicalCollection[C,K,R]]

  //The standard (non-transitive) reconstructive methods for tuples take precedence so that there is no ambiguity in the specific
  //case that we request a Binder for a tupled variable expression to apply to a tupled varaible expression, and so either route will work (though
  //both then delegate to the other in symmetry).
  //Also it seems likely that the transitivity puts a lot of strain on the compiler so the lower we push such a search down the tree
  //the easier we make it for the compiler.
  implicit def BindTuple2[
  V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], T1, T2, E1 <: VariableExpr[E1], E2 <: VariableExpr[E2], E3 <: VariableExpr[E3]
  ](implicit bind1: Binder[V1, T1, E1, E2], bind2: Binder[V2, T2, E2, E3]): Binder[Tuple2VariableExpr[V1, V2], (T1, T2), E1, E3] =
    new Binder[Tuple2VariableExpr[V1, V2], (T1, T2), E1, E3] {
      def apply(v1: E1) = bind2(bind1(v1))
    }

  implicit def BindTuple3[
  V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], V3 <: VariableExpr[V3], T1, T2, T3,
  E1 <: VariableExpr[E1], E2 <: VariableExpr[E2], E3 <: VariableExpr[E3], E4 <: VariableExpr[E4]
  ](implicit bind1: Binder[V1, T1, E1, E2],
    bind2: Binder[V2, T2, E2, E3],
    bind3: Binder[V3, T3, E3, E4]): Binder[Tuple3VariableExpr[V1, V2, V3], (T1, T2, T3), E1, E4] =
    new Binder[Tuple3VariableExpr[V1, V2, V3], (T1, T2, T3), E1, E4] {
      def apply(v1: E1) = bind3(bind2(bind1(v1)))
    }
}

trait Priority1BindingImplicits extends Priority0BindingImplicits {

  /** Standard inductive binder cases. */
  implicit def AddBinder[V <: VariableExpr[V], T, L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, AddExpr[L, R], AddExpr[L1, R1]] =
    new Binder[V, T, AddExpr[L, R], AddExpr[L1, R1]] {
      def apply(v1: AddExpr[L, R]) = AddExpr(bindL(v1.c1), bindR(v1.c2))
    }

  implicit def MultiplyBinder[V <: VariableExpr[V], T, L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, MultiplyExpr[L, R], MultiplyExpr[L1, R1]] =
    new Binder[V, T, MultiplyExpr[L, R], MultiplyExpr[L1, R1]] {
      def apply(v1: MultiplyExpr[L, R]) = MultiplyExpr(bindL(v1.c1), bindR(v1.c2))
    }

  implicit def DotBinder[V <: VariableExpr[V], T, L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, DotExpr[L, R], DotExpr[L1, R1]] =
    new Binder[V, T, DotExpr[L, R], DotExpr[L1, R1]] {
      def apply(v1: DotExpr[L, R]) = DotExpr(bindL(v1.c1), bindR(v1.c2))
    }

  implicit def JoinBinder[V <: VariableExpr[V], T, L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit bindL: Binder[V, T, L, L1], bindR: Binder[V, T, R, R1]): Binder[V, T, JoinExpr[L, R], JoinExpr[L1, R1]] =
    new Binder[V, T, JoinExpr[L, R], JoinExpr[L1, R1]] {
      def apply(v1: JoinExpr[L, R]) = JoinExpr(bindL(v1.c1), bindR(v1.c2))
    }

  //This applies a single binder to a Tuple2 variable expression.

  implicit def NotBinder[V <: VariableExpr[V], T, R <: RingExpr, R1 <: RingExpr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, NotExpr[R], NotExpr[R1]] =
    new Binder[V, T, NotExpr[R], NotExpr[R1]] {
      def apply(v1: NotExpr[R]) = NotExpr(recur(v1.c1))
    }

  implicit def NegateBinder[V <: VariableExpr[V], T, R <: RingExpr, R1 <: RingExpr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, NegateExpr[R], NegateExpr[R1]] =
    new Binder[V, T, NegateExpr[R], NegateExpr[R1]] {
      def apply(v1: NegateExpr[R]) = NegateExpr(recur(v1.c1))
    }

  implicit def SumBinder[V <: VariableExpr[V], T, R <: RingExpr, R1 <: RingExpr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, SumExpr[R], SumExpr[R1]] =
    new Binder[V, T, SumExpr[R], SumExpr[R1]] {
      def apply(v1: SumExpr[R]) = SumExpr(recur(v1.c1))
    }

  implicit def GroupBinder[V <: VariableExpr[V], T, R <: RingExpr, R1 <: RingExpr]
  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, GroupExpr[R], GroupExpr[R1]] =
    new Binder[V, T, GroupExpr[R], GroupExpr[R1]] {
      def apply(v1: GroupExpr[R]) = GroupExpr(recur(v1.c1))
    }

  implicit def SngBinder[V <: VariableExpr[V], T, K <: KeyExpr, R <: RingExpr, K1 <: KeyExpr, R1 <: RingExpr]
  (implicit bindK: Binder[V, T, K, K1], bindR: Binder[V, T, R, R1]): Binder[V, T, SngExpr[K, R], SngExpr[K1, R1]] =
    new Binder[V, T, SngExpr[K, R], SngExpr[K1, R1]] {
      def apply(v1: SngExpr[K, R]) = SngExpr(bindK(v1.key), bindR(v1.value))
    }

  implicit def EqualsPredicateBinder[V <: VariableExpr[V], T, K1 <: KeyExpr, K2 <: KeyExpr, K1B <: KeyExpr, K2B <: KeyExpr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B]): Binder[V, T, EqualsPredicate[K1, K2], EqualsPredicate[K1B, K2B]] =
    new Binder[V, T, EqualsPredicate[K1, K2], EqualsPredicate[K1B, K2B]] {
      def apply(v1: EqualsPredicate[K1, K2]) = EqualsPredicate(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def IntPredicateBinder[V <: VariableExpr[V], T, K1 <: KeyExpr, K2 <: KeyExpr, K1B <: KeyExpr, K2B <: KeyExpr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B]): Binder[V, T, IntPredicate[K1, K2], IntPredicate[K1B, K2B]] =
    new Binder[V, T, IntPredicate[K1, K2], IntPredicate[K1B, K2B]] {
      def apply(v1: IntPredicate[K1, K2]) = IntPredicate(bind1(v1.c1), bind2(v1.c2), v1.p, v1.opString)
    }

  implicit def ToRingBinder[V <: VariableExpr[V], T, R <: Expr, R1 <: Expr]
  (implicit bind: Binder[V, T, R, R1]): Binder[V, T, ToRingExpr[R], ToRingExpr[R1]] =
    new Binder[V, T, ToRingExpr[R], ToRingExpr[R1]] {
      def apply(v1: ToRingExpr[R]) = ToRingExpr(bind(v1.c1))
    }

  implicit def BoxedRingBinder[V <: VariableExpr[V], T, R <: Expr, R1 <: RingExpr]
  (implicit bind: Binder[V, T, R, R1]): Binder[V, T, BoxedRingExpr[R], BoxedRingExpr[R1]] =
    new Binder[V, T, BoxedRingExpr[R], BoxedRingExpr[R1]] {
      def apply(v1: BoxedRingExpr[R]) = BoxedRingExpr(bind(v1.c1))
    }

  implicit def LabelBinder[V <: VariableExpr[V], T, R <: RingExpr, R1 <: RingExpr]
  (implicit bind: Binder[V, T, R, R1]): Binder[V, T, LabelExpr[R], LabelExpr[R1]] =
    new Binder[V, T, LabelExpr[R], LabelExpr[R1]] {
      def apply(v1: LabelExpr[R]) = LabelExpr(bind(v1.c1))
    }

  //todo - do these need to be higher priority than the key binder methods? and would the key methods need to be lower prioirty
  //than the transitive methods?
  implicit def Tuple2VariableBinder[
  V <: VariableExpr[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2],
  V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B]
  ](implicit bind1: Binder[V, T, V1, V1B], bind2: Binder[V, T, V2, V2B]):
  Binder[V, T, Tuple2VariableExpr[V1, V2], Tuple2VariableExpr[V1B, V2B]] =
    new Binder[V, T, Tuple2VariableExpr[V1, V2], Tuple2VariableExpr[V1B, V2B]] {
      def apply(v1: Tuple2VariableExpr[V1, V2]) = Tuple2VariableExpr(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def Tuple3VariableBinder[
  V <: VariableExpr[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], V3 <: VariableExpr[V3],
  V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B], V3B <: VariableExpr[V3B]
  ](implicit bind1: Binder[V, T, V1, V1B], bind2: Binder[V, T, V2, V2B], bind3: Binder[V, T, V3, V3B]):
  Binder[V, T, Tuple3VariableExpr[V1, V2, V3], Tuple3VariableExpr[V1B, V2B, V3B]] =
    new Binder[V, T, Tuple3VariableExpr[V1, V2, V3], Tuple3VariableExpr[V1B, V2B, V3B]] {
      def apply(v1: Tuple3VariableExpr[V1, V2, V3]) = Tuple3VariableExpr(bind1(v1.c1), bind2(v1.c2), bind3(v1.c3))
    }

  implicit def Tuple2KeyBinder[V <: VariableExpr[V], T, K1 <: KeyExpr, K2 <: KeyExpr, K1B <: KeyExpr, K2B <: KeyExpr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B]): Binder[V, T, Tuple2KeyExpr[K1, K2], Tuple2KeyExpr[K1B, K2B]] =
    new Binder[V, T, Tuple2KeyExpr[K1, K2], Tuple2KeyExpr[K1B, K2B]] {
      def apply(v1: Tuple2KeyExpr[K1, K2]) = Tuple2KeyExpr(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def Tuple3KeyBinder[V <: VariableExpr[V], T, K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr, K1B <: KeyExpr, K2B <: KeyExpr, K3B <: KeyExpr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B], bind3: Binder[V, T, K3, K3B]): Binder[V, T, Tuple3KeyExpr[K1, K2, K3], Tuple3KeyExpr[K1B, K2B, K3B]] =
    new Binder[V, T, Tuple3KeyExpr[K1, K2, K3], Tuple3KeyExpr[K1B, K2B, K3B]] {
      def apply(v1: Tuple3KeyExpr[K1, K2, K3]) = Tuple3KeyExpr(bind1(v1.c1), bind2(v1.c2), bind3(v1.c3))
    }

  implicit def Tuple2RingBinder[V <: VariableExpr[V], T, K1 <: RingExpr, K2 <: RingExpr, K1B <: RingExpr, K2B <: RingExpr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B]): Binder[V, T, Tuple2RingExpr[K1, K2], Tuple2RingExpr[K1B, K2B]] =
    new Binder[V, T, Tuple2RingExpr[K1, K2], Tuple2RingExpr[K1B, K2B]] {
      def apply(v1: Tuple2RingExpr[K1, K2]) = Tuple2RingExpr(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def Tuple3RingBinder[V <: VariableExpr[V], T, K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr, K1B <: RingExpr, K2B <: RingExpr, K3B <: RingExpr]
  (implicit bind1: Binder[V, T, K1, K1B], bind2: Binder[V, T, K2, K2B], bind3: Binder[V, T, K3, K3B]): Binder[V, T, Tuple3RingExpr[K1, K2, K3], Tuple3RingExpr[K1B, K2B, K3B]] =
    new Binder[V, T, Tuple3RingExpr[K1, K2, K3], Tuple3RingExpr[K1B, K2B, K3B]] {
      def apply(v1: Tuple3RingExpr[K1, K2, K3]) = Tuple3RingExpr(bind1(v1.c1), bind2(v1.c2), bind3(v1.c3))
    }

//  implicit def Project1RingBinder[V <: VariableExpr[V], T, R <: RingExpr with C1Expr, R1 <: RingExpr with C1Expr]
//  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, Project1RingExpr[R], Project1RingExpr[R1]] =
//    new Binder[V, T, Project1RingExpr[R], Project1RingExpr[R1]] {
//      def apply(v1: Project1RingExpr[R]) = Project1RingExpr(recur(v1.c1))
//    }
//
//  implicit def Project2RingBinder[V <: VariableExpr[V], T, R <: RingExpr with C2Expr, R1 <: RingExpr with C2Expr]
//  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, Project2RingExpr[R], Project2RingExpr[R1]] =
//    new Binder[V, T, Project2RingExpr[R], Project2RingExpr[R1]] {
//      def apply(v1: Project2RingExpr[R]) = Project2RingExpr(recur(v1.c1))
//    }
//
//  implicit def Project3RingBinder[V <: VariableExpr[V], T, R <: RingExpr with C3Expr, R1 <: RingExpr with C3Expr]
//  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, Project3RingExpr[R], Project3RingExpr[R1]] =
//    new Binder[V, T, Project3RingExpr[R], Project3RingExpr[R1]] {
//      def apply(v1: Project3RingExpr[R]) = Project3RingExpr(recur(v1.c1))
//    }
//
//  implicit def Project1KeyBinder[V <: VariableExpr[V], T, R <: KeyExpr with C1Expr, R1 <: KeyExpr with C1Expr]
//  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, Project1KeyExpr[R], Project1KeyExpr[R1]] =
//    new Binder[V, T, Project1KeyExpr[R], Project1KeyExpr[R1]] {
//      def apply(v1: Project1KeyExpr[R]) = Project1KeyExpr(recur(v1.c1))
//    }
//
//  implicit def Project2KeyBinder[V <: VariableExpr[V], T, R <: KeyExpr with C2Expr, R1 <: KeyExpr with C2Expr]
//  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, Project2KeyExpr[R], Project2KeyExpr[R1]] =
//    new Binder[V, T, Project2KeyExpr[R], Project2KeyExpr[R1]] {
//      def apply(v1: Project2KeyExpr[R]) = Project2KeyExpr(recur(v1.c1))
//    }
//
//  implicit def Project3KeyBinder[V <: VariableExpr[V], T, R <: KeyExpr with C3Expr, R1 <: KeyExpr with C3Expr]
//  (implicit recur: Binder[V, T, R, R1]): Binder[V, T, Project3KeyExpr[R], Project3KeyExpr[R1]] =
//    new Binder[V, T, Project3KeyExpr[R], Project3KeyExpr[R1]] {
//      def apply(v1: Project3KeyExpr[R]) = Project3KeyExpr(recur(v1.c1))
//    }
}

trait BindingImplicits extends Priority1BindingImplicits {
  //Never try to find an actual binder for a typed variable.
  implicit def TypedNonBinder[V <: VariableExpr[V], T, T1]: Binder[V, T, TypedVariable[T1], TypedVariable[T1]] = Binder.nonBinder[V, T, TypedVariable[T1]]

  /** Binding base cases - primitive expressions don't need to bind anything, variables bind iff they are untyped variables
    * matching the type in the binder. (otherwise they use the low priority non-binding case)
    */
  implicit def PrimitiveKeyExprBinder[V <: VariableExpr[V], T, V1]: Binder[V, T, PrimitiveKeyExpr[V1], PrimitiveKeyExpr[V1]] =
    Binder.nonBinder[V, T, PrimitiveKeyExpr[V1]]

  implicit def NumericExprBinder[V <: VariableExpr[V], T, V1]: Binder[V, T, NumericExpr[V1],NumericExpr[V1]] =
    Binder.nonBinder[V, T, NumericExpr[V1]]

  implicit def VariableBinder[V <: UntypedVariable[V], T, V1 <: UntypedVariable[V1]](implicit ev: V1 <:< V): Binder[V, T, UntypedVariable[V1], TypedVariable[T]] =
    new Binder[V, T, UntypedVariable[V1], TypedVariable[T]] {
      def apply(v1: UntypedVariable[V1]): TypedVariable[T] = v1.tag[T]
    }

  /** Special binder for inner infinite mappings - dont attempt to bind the key */
  implicit def InfMappingBinder[V <: VariableExpr[V], T, K <: VariableExpr[K], R <: RingExpr, R1 <: RingExpr]
  (implicit bindR: Binder[V, T, R, R1]): Binder[V, T, InfiniteMappingExpr[K, R], InfiniteMappingExpr[K, R1]] =
    new Binder[V, T, InfiniteMappingExpr[K, R], InfiniteMappingExpr[K, R1]] {
      def apply(v1: InfiniteMappingExpr[K, R]) = InfiniteMappingExpr(v1.key, bindR(v1.value))
    }
}
