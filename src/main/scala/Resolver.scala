package slender

import shapeless._

/** Resolve an expression containing untyped variables to one in which all variables are typed,
  * so that it can be evaluated.
  *
  * Untyped variables may be introduced in an infinite mapping. When this infinite mapping is multiplied by
  * a finite collection, we can infer the types of these untyped variables using
  * the key type of the finite collection. E.g. if we have the expression X * ((x,y) => 1) and X is a Bag[(Int,String)],
  * we can infer that the variable x must have type Int and y must have type String.
  *
  * Thus, we can resolve such any expression by:
  *   A. If it's a multiplication between a finite collection and an infinite mapping:
  *     1. Recursively resolve the finite collection.
  *     2. Traversing the infinite mapping (key and value) and appropriately
  *        tagging each untyped variable. (i.e. tag with the matching type if it's an untyped variable introduced in
  *        the key, otherwise ignore).
  *     3. Recursively resolving the value of the infinite mapping.
  *
  *   B. If it's any other expression, simply recursively resolve its children and rebuild.
  */
trait Resolver[-In,+Out] extends (In => Out) with Serializable

object Resolver {

  def instance[In, Out](f: In => Out): Resolver[In,Out] = new Resolver[In,Out] {
    def apply(v1: In): Out = f(v1)
  }

  def nonResolver[E]: Resolver[E,E] = Resolver.instance { v1 => v1 }

  /**Resolve a multiplication of a finite collection on the LHS with an inf mapping on the RHS.
    * This is the 'main event' of variable resolution, and works as follows:
    * 1 - Resolve the left hand side to get an expres
    * 2 - Require that the LHS evaluates to a finite collection.
    * 3 - Bind the key of the infinite mapping on the RHS (which may be a nested product of variables)
    *     to the discovered key type of the finite collection.
    * 4 - Recursively bind all instances of those variables in the value of the infinite mapping.
    * 5 - Recursively resolve the value of the infinite mapping.*/
  implicit def MultiplyInfResolver[
    LHS <: Expr, LHS1 <: Expr, V <: Expr, C[_,_], KT, VB <: Expr,
    RT, R1 <: Expr, R2 <: Expr, R3 <: Expr
  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C[KT,RT]], coll: Collection[C,KT,RT],
    tagLeft: Tagger[V,KT,V,VB], tagRight: Tagger[V,KT,R1,R2], resolver: Resolver[R2,R3]):
  Resolver[
    MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
    MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
    ] =
    new Resolver[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
      MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
      ] {
      def apply(v1: MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]]) =
        MultiplyExpr(resolveLeft(v1.c1),InfiniteMappingExpr(tagLeft(v1.c2.key),resolver(tagRight(v1.c2.value))))
    }
}

trait Priority0ResolutionImplicits {

  implicit def NumericExprResolver[V]: Resolver[NumericExpr[V],NumericExpr[V]] =
    Resolver.nonResolver[NumericExpr[V]]

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
  implicit def AddResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[AddExpr[L,R],AddExpr[L1,R1]] =
    new Resolver[AddExpr[L,R],AddExpr[L1,R1]] {
      def apply(v1: AddExpr[L,R]) = AddExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def MultiplyResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] {
      def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def JoinResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[JoinExpr[L,R],JoinExpr[L1,R1]] =
    new Resolver[JoinExpr[L,R],JoinExpr[L1,R1]] {
      def apply(v1: JoinExpr[L,R]) = JoinExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def DotResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[DotExpr[L,R],DotExpr[L1,R1]] =
    new Resolver[DotExpr[L,R],DotExpr[L1,R1]] {
      def apply(v1: DotExpr[L,R]) = DotExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def NotResolver[R1 <: Expr,R2 <: Expr](implicit resolver: Resolver[R1,R2]):
  Resolver[NotExpr[R1],NotExpr[R2]] = new Resolver[NotExpr[R1],NotExpr[R2]] {
    def apply(v1: NotExpr[R1]): NotExpr[R2] = NotExpr(resolver(v1.c1))
  }

  implicit def NegateResolver[R1 <: Expr,R2 <: Expr](implicit resolver: Resolver[R1,R2]):
  Resolver[NegateExpr[R1],NegateExpr[R2]] = new Resolver[NegateExpr[R1],NegateExpr[R2]] {
    def apply(v1: NegateExpr[R1]): NegateExpr[R2] = NegateExpr(resolver(v1.c1))
  }

  implicit def SumResolver[R1 <: Expr,R2 <: Expr](implicit resolver: Resolver[R1,R2]):
  Resolver[SumExpr[R1],SumExpr[R2]] = new Resolver[SumExpr[R1],SumExpr[R2]] {
    def apply(v1: SumExpr[R1]): SumExpr[R2] = SumExpr(resolver(v1.c1))
  }

  implicit def GroupResolver[R1 <: Expr,R2 <: Expr](implicit resolver: Resolver[R1,R2]):
  Resolver[GroupExpr[R1],GroupExpr[R2]] = new Resolver[GroupExpr[R1],GroupExpr[R2]] {
    def apply(v1: GroupExpr[R1]): GroupExpr[R2] = GroupExpr(resolver(v1.c1))
  }

  implicit def SngResolver[K <: Expr,R <: Expr,K1 <: Expr,R1 <: Expr]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    new Resolver[SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(resolveK(v1.key),resolveR(v1.value)) }

  implicit def EqualsPredicateResolver[K1 <: Expr,K2 <: Expr,K1B <: Expr,K2B <: Expr]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] =
    new Resolver[EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] {
      def apply(v1: EqualsPredicate[K1,K2]) = EqualsPredicate(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def IntPredicateResolver[K1 <: Expr,K2 <: Expr,K1B <: Expr,K2B <: Expr]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[IntPredicate[K1,K2],IntPredicate[K1B,K2B]] =
    new Resolver[IntPredicate[K1,K2],IntPredicate[K1B,K2B]] {
      def apply(v1: IntPredicate[K1,K2]) = IntPredicate(resolve1(v1.c1),resolve2(v1.c2), v1.p, v1.opString)
    }

  implicit def ProductResolver[C <: HList, CR <: HList]
  (implicit resolve: Resolver[C,CR], lub: LUBConstraint[CR,Expr]): Resolver[ProductExpr[C],ProductExpr[CR]] =
    new Resolver[ProductExpr[C],ProductExpr[CR]] {
      def apply(v1: ProductExpr[C]) = ProductExpr(resolve(v1.exprs))
    }

  implicit def HNilResolver: Resolver[HNil,HNil] = Resolver.nonResolver[HNil]

  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[T1, T2]):
  Resolver[H1 :: T1, H2 :: T2] = new Resolver[H1 :: T1, H2 :: T2] {
    def apply(v1: H1 :: T1): H2 :: T2 = v1 match {
      case h1 :: t1 => resolveH(h1) :: resolveT(t1)
    }
  }
}