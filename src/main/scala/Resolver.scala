package slender

import shapeless._
import shapeless.ops.hlist.ToTraversable

trait Resolver[-In,+Out] extends (In => Out) with Serializable

//trait HResolver[-In,+Out] extends (In => Out) with Serializable

object Resolver extends ResolutionImplicits {

  def instance[In, Out](f: In => Out): Resolver[In,Out] = new Resolver[In,Out] {
    def apply(v1: In): Out = f(v1)
  }

  def nonResolver[E]: Resolver[E,E] = new Resolver[E,E] { def apply(v1: E) = v1 }
}

//object HResolver {
//
//  implicit def HNilResolver: HResolver[HNil,HNil] = new HResolver[HNil,HNil] {
//    def apply(v1: HNil): HNil = v1
//  }
//
//  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
//  (implicit resolveH: Resolver[H1, H2], resolveT: HResolver[T1, T2]):
//  HResolver[H1 :: T1, H2 :: T2] = new HResolver[H1 :: T1, H2 :: T2] {
//    def apply(v1: H1 :: T1): H2 :: T2 = v1 match {
//      case h1 :: t1 => resolveH(h1) :: resolveT(t1)
//    }
//  }
//
//}
trait Priority0ResolutionImplicits {

  implicit def NumericExprResolver[V]: Resolver[NumericExpr[V],NumericExpr[V]] =
    Resolver.nonResolver[NumericExpr[V]]

//  implicit def InductiveResolver[
//    E1 <: Expr,E2 <: Expr, Repr1 <: HList, Repr2 <: HList
//  ]
//  (implicit gen1: Generic.Aux[E1,Repr1], resolve: Resolver[Repr1,Repr2],
//   ev: Reconstruct[E1, Repr2, E2], gen2: Generic.Aux[E2, Repr2]):
//  Resolver[E1, E2] = new Resolver[E1, E2] {
//    def apply(v1: E1): E2 = gen2.from(resolve(gen1.to(v1)))
//  }
//
//  implicit def HNilResolver: Resolver[HNil,HNil] = new Resolver[HNil,HNil] {
//    def apply(v1: HNil): HNil = v1
//  }
//
//  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
//  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[T1, T2]):
//  Resolver[H1 :: T1, H2 :: T2] = new Resolver[H1 :: T1, H2 :: T2] {
//    def apply(v1: H1 :: T1): H2 :: T2 = v1 match {
//      case h1 :: t1 => resolveH(h1) :: resolveT(t1)
//    }
//  }
}

trait Priority1ResolutionImplicits extends Priority0ResolutionImplicits {
  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def PrimitiveKeyExprResolver[V]: Resolver[PrimitiveKeyExpr[V],PrimitiveKeyExpr[V]] =
    Resolver.nonResolver[PrimitiveKeyExpr[V]]

  implicit def PhysicalCollectionResolver[C[_,_],K,R]: Resolver[PhysicalCollection[C,K,R],PhysicalCollection[C,K,R]] =
    Resolver.nonResolver[PhysicalCollection[C,K,R]]

  implicit def VariableResolver[V <: UntypedVariable,T]: Resolver[TypedVariable[T],TypedVariable[T]] =
    Resolver.nonResolver[TypedVariable[T]]

}

trait Priority2ResolutionImplicits extends Priority1ResolutionImplicits {
  /**Standard inductive cases*/
  //Ring expresssions
  implicit def AddResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[AddExpr[L,R],AddExpr[L1,R1]] =
    new Resolver[AddExpr[L,R],AddExpr[L1,R1]] {
      def apply(v1: AddExpr[L,R]) = AddExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def MultiplyResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] {
      def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def JoinResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[JoinExpr[L,R],JoinExpr[L1,R1]] =
    new Resolver[JoinExpr[L,R],JoinExpr[L1,R1]] {
      def apply(v1: JoinExpr[L,R]) = JoinExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def DotResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[DotExpr[L,R],DotExpr[L1,R1]] =
    new Resolver[DotExpr[L,R],DotExpr[L1,R1]] {
      def apply(v1: DotExpr[L,R]) = DotExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def NotResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
  Resolver[NotExpr[R1],NotExpr[R2]] = new Resolver[NotExpr[R1],NotExpr[R2]] {
    def apply(v1: NotExpr[R1]): NotExpr[R2] = NotExpr(resolver(v1.c1))
  }

  implicit def NegateResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
  Resolver[NegateExpr[R1],NegateExpr[R2]] = new Resolver[NegateExpr[R1],NegateExpr[R2]] {
    def apply(v1: NegateExpr[R1]): NegateExpr[R2] = NegateExpr(resolver(v1.c1))
  }

  implicit def SumResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
  Resolver[SumExpr[R1],SumExpr[R2]] = new Resolver[SumExpr[R1],SumExpr[R2]] {
    def apply(v1: SumExpr[R1]): SumExpr[R2] = SumExpr(resolver(v1.c1))
  }

  implicit def GroupResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
  Resolver[GroupExpr[R1],GroupExpr[R2]] = new Resolver[GroupExpr[R1],GroupExpr[R2]] {
    def apply(v1: GroupExpr[R1]): GroupExpr[R2] = GroupExpr(resolver(v1.c1))
  }

  implicit def SngResolver[K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    new Resolver[SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(resolveK(v1.key),resolveR(v1.value)) }

  implicit def EqualsPredicateResolver[K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] =
    new Resolver[EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] {
      def apply(v1: EqualsPredicate[K1,K2]) = EqualsPredicate(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def IntPredicateResolver[K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[IntPredicate[K1,K2],IntPredicate[K1B,K2B]] =
    new Resolver[IntPredicate[K1,K2],IntPredicate[K1B,K2B]] {
      def apply(v1: IntPredicate[K1,K2]) = IntPredicate(resolve1(v1.c1),resolve2(v1.c2), v1.p, v1.opString)
    }

  implicit def ToRingResolver[R <: Expr,R1 <: Expr]
  (implicit resolve: Resolver[R,R1]):Resolver[ToRingExpr[R],ToRingExpr[R1]] =
    new Resolver[ToRingExpr[R],ToRingExpr[R1]] { def apply(v1: ToRingExpr[R]) = ToRingExpr(resolve(v1.c1)) }

  //Key expressions
  implicit def BoxedRingResolver[R <: Expr,R1 <: RingExpr]
  (implicit resolve: Resolver[R,R1]):Resolver[BoxedRingExpr[R],BoxedRingExpr[R1]] =
    new Resolver[BoxedRingExpr[R],BoxedRingExpr[R1]] { def apply(v1: BoxedRingExpr[R]) = BoxedRingExpr(resolve(v1.c1)) }

  implicit def LabelResolver[R <: RingExpr,R1 <: RingExpr]
  (implicit resolve: Resolver[R,R1]):Resolver[LabelExpr[R],LabelExpr[R1]] =
    new Resolver[LabelExpr[R],LabelExpr[R1]] { def apply(v1: LabelExpr[R]) = LabelExpr(resolve(v1.c1)) }


  //Tupling and projection
//  implicit def Tuple2RingResolver[K1 <: RingExpr,K2 <: RingExpr,K1B <: RingExpr,K2B <: RingExpr]
//  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[Tuple2RingExpr[K1,K2],Tuple2RingExpr[K1B,K2B]] =
//    new Resolver[Tuple2RingExpr[K1,K2],Tuple2RingExpr[K1B,K2B]] {
//      def apply(v1: Tuple2RingExpr[K1,K2]) = Tuple2RingExpr(resolve1(v1.c1),resolve2(v1.c2))
//    }
//
//  implicit def Tuple2KeyResolver[K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
//  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[Tuple2KeyExpr[K1,K2],Tuple2KeyExpr[K1B,K2B]] =
//    new Resolver[Tuple2KeyExpr[K1,K2],Tuple2KeyExpr[K1B,K2B]] {
//      def apply(v1: Tuple2KeyExpr[K1,K2]) = Tuple2KeyExpr(resolve1(v1.c1),resolve2(v1.c2))
//    }

//  implicit def Tuple2VariableResolver[K1 <: VariableExpr,K2 <: VariableExpr,K1B <: VariableExpr,K2B <: VariableExpr]
//  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[Tuple2VariableExpr[K1,K2],Tuple2VariableExpr[K1B,K2B]] =
//    new Resolver[Tuple2VariableExpr[K1,K2],Tuple2VariableExpr[K1B,K2B]] {
//      def apply(v1: Tuple2VariableExpr[K1,K2]) = Tuple2VariableExpr(resolve1(v1.c1),resolve2(v1.c2))
//    }
//
////  implicit def Tuple3RingResolver[K1 <: RingExpr,K2 <: RingExpr,K3 <: RingExpr,K1B <: RingExpr,K2B <: RingExpr,K3B <: RingExpr]
////  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B], resolve3: Resolver[K3,K3B]): Resolver[Tuple3RingExpr[K1,K2,K3],Tuple3RingExpr[K1B,K2B,K3B]] =
////    new Resolver[Tuple3RingExpr[K1,K2,K3],Tuple3RingExpr[K1B,K2B,K3B]] {
////      def apply(v1: Tuple3RingExpr[K1,K2,K3]) = Tuple3RingExpr(resolve1(v1.c1),resolve2(v1.c2),resolve3(v1.c3))
////    }
////
////  implicit def Tuple3KeyResolver[K1 <: KeyExpr,K2 <: KeyExpr,K3 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr,K3B <: KeyExpr]
////  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B], resolve3: Resolver[K3,K3B]): Resolver[Tuple3KeyExpr[K1,K2,K3],Tuple3KeyExpr[K1B,K2B,K3B]] =
////    new Resolver[Tuple3KeyExpr[K1,K2,K3],Tuple3KeyExpr[K1B,K2B,K3B]] {
////      def apply(v1: Tuple3KeyExpr[K1,K2,K3]) = Tuple3KeyExpr(resolve1(v1.c1),resolve2(v1.c2),resolve3(v1.c3))
////    }
//
//  implicit def Tuple3VariableResolver[
//  K1 <: VariableExpr,K2 <: VariableExpr,K3 <: VariableExpr,
//  K1B <: VariableExpr,K2B <: VariableExpr,K3B <: VariableExpr
//  ](implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B], resolve3: Resolver[K3,K3B]):
//  Resolver[Tuple3VariableExpr[K1,K2,K3],Tuple3VariableExpr[K1B,K2B,K3B]] =
//    new Resolver[Tuple3VariableExpr[K1,K2,K3],Tuple3VariableExpr[K1B,K2B,K3B]] {
//      def apply(v1: Tuple3VariableExpr[K1,K2,K3]) = Tuple3VariableExpr(resolve1(v1.c1),resolve2(v1.c2),resolve3(v1.c3))
//    }

  implicit def ProductKeyResolver[C <: HList, CR <: HList]
  (implicit resolve: Resolver[C,CR], lub: LUBConstraint[CR,KeyExpr]): Resolver[ProductKeyExpr[C],ProductKeyExpr[CR]] =
    new Resolver[ProductKeyExpr[C],ProductKeyExpr[CR]] {
      def apply(v1: ProductKeyExpr[C]) = ProductKeyExpr(resolve(v1.exprs))
    }

  implicit def ProductRingResolver[C <: HList, CR <: HList]
  (implicit resolve: Resolver[C,CR], lub: LUBConstraint[CR,RingExpr]): Resolver[ProductRingExpr[C],ProductRingExpr[CR]] =
    new Resolver[ProductRingExpr[C],ProductRingExpr[CR]] {
      def apply(v1: ProductRingExpr[C]) = ProductRingExpr(resolve(v1.exprs))
    }

  implicit def ProductVariableResolver[C <: HList, CR <: HList]
  (implicit resolve: Resolver[C,CR], lub: LUBConstraint[CR,VariableExpr]): Resolver[ProductVariableExpr[C],ProductVariableExpr[CR]] =
    new Resolver[ProductVariableExpr[C],ProductVariableExpr[CR]] {
      def apply(v1: ProductVariableExpr[C]) = ProductVariableExpr(resolve(v1.exprs))
    }

  implicit def HNilResolver: Resolver[HNil,HNil] = Resolver.nonResolver[HNil]

  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[T1, T2]):
  Resolver[H1 :: T1, H2 :: T2] = new Resolver[H1 :: T1, H2 :: T2] {
    def apply(v1: H1 :: T1): H2 :: T2 = v1 match {
      case h1 :: t1 => resolveH(h1) :: resolveT(t1)
    }
  }


  //  implicit def Project1RingResolver[R1 <: RingExpr with C1Expr,R2 <: RingExpr with C1Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project1RingExpr[R1],Project1RingExpr[R2]] = new Resolver[Project1RingExpr[R1],Project1RingExpr[R2]] {
//    def apply(v1: Project1RingExpr[R1]): Project1RingExpr[R2] = Project1RingExpr(resolver(v1.c1))
//  }
//
//  implicit def Project1KeyResolver[R1 <: KeyExpr with C1Expr,R2 <: KeyExpr with C1Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project1KeyExpr[R1],Project1KeyExpr[R2]] = new Resolver[Project1KeyExpr[R1],Project1KeyExpr[R2]] {
//    def apply(v1: Project1KeyExpr[R1]): Project1KeyExpr[R2] = Project1KeyExpr(resolver(v1.c1))
//  }
//
//  implicit def Project2RingResolver[R1 <: RingExpr with C2Expr,R2 <: RingExpr with C2Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project2RingExpr[R1],Project2RingExpr[R2]] = new Resolver[Project2RingExpr[R1],Project2RingExpr[R2]] {
//    def apply(v1: Project2RingExpr[R1]): Project2RingExpr[R2] = Project2RingExpr(resolver(v1.c1))
//  }
//
//  implicit def Project2KeyResolver[R1 <: KeyExpr with C2Expr,R2 <: KeyExpr with C2Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project2KeyExpr[R1],Project2KeyExpr[R2]] = new Resolver[Project2KeyExpr[R1],Project2KeyExpr[R2]] {
//    def apply(v1: Project2KeyExpr[R1]): Project2KeyExpr[R2] = Project2KeyExpr(resolver(v1.c1))
//  }
//
//  implicit def Project3RingResolver[R1 <: RingExpr with C3Expr,R2 <: RingExpr with C3Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project3RingExpr[R1],Project3RingExpr[R2]] = new Resolver[Project3RingExpr[R1],Project3RingExpr[R2]] {
//    def apply(v1: Project3RingExpr[R1]): Project3RingExpr[R2] = Project3RingExpr(resolver(v1.c1))
//  }
//
//  implicit def Project3KeyResolver[R1 <: KeyExpr with C3Expr,R2 <: KeyExpr with C3Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project3KeyExpr[R1],Project3KeyExpr[R2]] = new Resolver[Project3KeyExpr[R1],Project3KeyExpr[R2]] {
//    def apply(v1: Project3KeyExpr[R1]): Project3KeyExpr[R2] = Project3KeyExpr(resolver(v1.c1))
//  }
}


trait ResolutionImplicits extends Priority2ResolutionImplicits {
  //Note that the below doesnt strictly need to be here - as a there is no standalone resolver for an infinite mapping,
  //the standard MultiplyResolver if tried for MultiplyExpr with an InfMapping on the RHS will fail. However, it's logical
  //to aid the compiler by making it try this one first for such expressions.
  /**Resolve a multiplication of a finite collection on the LHS with an inf mapping on the RHS.
    * This is the 'main event' of variable resolution, and works as follows:
    * 1 - Resolve the LHS
    * 2 - Require that the LHS evaluates to a finite collection.
    * 3 - Bind the key of the infinite mapping on the RHS (which may be a nested product of variables)
    *     to the discovered key type of the finite collection.
    * 4 - Recursively bind all instances of those variables in the value of the infinite mapping.
    * 5 - Recursively resolve the value of the infinite mapping.*/
  implicit def MultiplyInfResolver[
  LHS <: RingExpr, LHS1 <: RingExpr, V <: VariableExpr, C[_,_], KT, VB <: VariableExpr,
  RT, R1 <: RingExpr, R2 <: RingExpr, R3 <: RingExpr
  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C[KT,RT]], coll: Collection[C,KT,RT],
    bindLeft: Binder[V,KT,V,VB], bindRight: Binder[V,KT,R1,R2], resolver: Resolver[R2,R3]):
  Resolver[
    MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
    MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
    ] =
    new Resolver[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
      MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
      ] {
      def apply(v1: MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]]) =
        MultiplyExpr(resolveLeft(v1.c1),InfiniteMappingExpr(bindLeft(v1.c2.key),resolver(bindRight(v1.c2.value))))
    }
}