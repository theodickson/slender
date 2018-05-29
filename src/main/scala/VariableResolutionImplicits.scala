package slender

/**Base trait for resolvers.
  * Instances of ResolverBase may be provided by the implicit CantResolve method, which which will be provide a
  * 'do-nothing' resolver for any expression.
  * Instances of Resolver are only provided by methods which construct fully correct resolvers. I.e. those whose output is
  * an expression without any untyped variables.
  *
  * This split exists so that the main 'resolve' method on an expression can require a correct resolver. Thus explicit
  * uses of '.resolve' will not compile unless the expression can genuinely be resolved.
  *
  * However, the For comprehension consturct, which we would like to automatically resolve its output expressions,
  * must therefore be able to call resolve on its output without being able to fully resolve the output, since this
  * will not be possible for inner For comprehensions which reference outer variables.
  *
  * This has the side-effect of not being able to tell if the result of an outermost For comprehension is actually
  * resolved without either calling 'isResolved' or explicitly re-resolving, but its a good interim solution.
*/
trait ResolverBase[-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

trait Resolver[-In <: Expr,+Out <: Expr] extends ResolverBase[In,Out]

trait Binder[V <: VariableExpr[V],T,-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

object Resolver {
  def nonResolver[E <: Expr]: Resolver[E,E] = new Resolver[E,E] { def apply(v1: E) = v1 }
}

object Binder {
  def nonBinder[V <: VariableExpr[V], T, E <: Expr]: Binder[V,T,E,E] =
    new Binder[V,T,E,E] { def apply(v1: E) = v1 }
}


trait LowPriorityResolutionImplicits {
  //A default ResolverBase for any expression, so that for-comprehensions in the DSL can resolve expressions before
  //returning them (inner expressions may not be resolvable when initially returned).
  //Note that the other methods here and the main Resolve method require a Resolver not a ResolverBase so none of
  //those can use this one.
  implicit def CantResolve[E <: Expr]: ResolverBase[E,E] = new ResolverBase[E,E] { def apply(v1: E) = v1 }
}

trait StandardPriorityResolutionImplicits extends LowPriorityResolutionImplicits {
  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def PrimitiveExprResolver[E <: PrimitiveExpr[_]]: Resolver[E,E] = Resolver.nonResolver[E]

  implicit def VariableResolver[V <: UntypedVariable[V],T]: Resolver[TypedVariable[T],TypedVariable[T]] =
    Resolver.nonResolver[TypedVariable[T]]

  /**Standard inductive cases*/
  implicit def SumResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
  Resolver[SumExpr[R1],SumExpr[R2]] = new Resolver[SumExpr[R1],SumExpr[R2]] {
    def apply(v1: SumExpr[R1]): SumExpr[R2] = SumExpr(resolver(v1.c1))
  }

  implicit def SngResolver[K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    new Resolver[SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(resolveK(v1.key),resolveR(v1.value)) }


  implicit def MultiplyResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] {
      def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def DotResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[DotExpr[L,R],DotExpr[L1,R1]] =
    new Resolver[DotExpr[L,R],DotExpr[L1,R1]] {
      def apply(v1: DotExpr[L,R]) = DotExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def ToRingResolver[R <: Expr,R1 <: Expr]
  (implicit resolve: Resolver[R,R1]):Resolver[ToRingExpr[R],ToRingExpr[R1]] =
    new Resolver[ToRingExpr[R],ToRingExpr[R1]] { def apply(v1: ToRingExpr[R]) = ToRingExpr(resolve(v1.c1)) }

  implicit def BoxedRingResolver[R <: Expr,R1 <: RingExpr]
  (implicit resolve: Resolver[R,R1]):Resolver[BoxedRingExpr[R],BoxedRingExpr[R1]] =
    new Resolver[BoxedRingExpr[R],BoxedRingExpr[R1]] { def apply(v1: BoxedRingExpr[R]) = BoxedRingExpr(resolve(v1.c1)) }


  implicit def Tuple2KeyResolver[K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[Tuple2KeyExpr[K1,K2],Tuple2KeyExpr[K1B,K2B]] =
    new Resolver[Tuple2KeyExpr[K1,K2],Tuple2KeyExpr[K1B,K2B]] {
      def apply(v1: Tuple2KeyExpr[K1,K2]) = Tuple2KeyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def Tuple2VariableResolver[K1 <: VariableExpr[K1],K2 <: VariableExpr[K2],K1B <: VariableExpr[K1B],K2B <: VariableExpr[K2B]]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[Tuple2VariableExpr[K1,K2],Tuple2VariableExpr[K1B,K2B]] =
    new Resolver[Tuple2VariableExpr[K1,K2],Tuple2VariableExpr[K1B,K2B]] {
      def apply(v1: Tuple2VariableExpr[K1,K2]) = Tuple2VariableExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def EqualsPredicateResolver[K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit resolve1: Resolver[K1,K1B], resolve2: Resolver[K2,K2B]): Resolver[EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] =
    new Resolver[EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] {
      def apply(v1: EqualsPredicate[K1,K2]) = EqualsPredicate(resolve1(v1.c1),resolve2(v1.c2))
    }
}


trait LowPriorityBindingImplicits {
  //The case that V1 <:< V and so actually binds to T takes precedence.
  implicit def VariableNonBinder[V <: UntypedVariable[V],V1 <: UntypedVariable[V1],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]

  //The standard (non-transitive) reconstructive methods for tuples take precedence so that there is no ambiguity in the specific
  //case that we are trying to bind a tupled variable expression to one of the same arity and so either route will work (though
  //both then delegate to the other in symmetry).
  implicit def BindTuple2[
    V1<:VariableExpr[V1],V2<:VariableExpr[V2],T1,T2,E1<:VariableExpr[E1],E2<:VariableExpr[E2],E3<:VariableExpr[E3]
  ](implicit bind1: Binder[V1,T1,E1,E2], bind2: Binder[V2,T2,E2,E3]): Binder[Tuple2VariableExpr[V1,V2],(T1,T2),E1,E3] =
    new Binder[Tuple2VariableExpr[V1,V2],(T1,T2),E1,E3] {
      def apply(v1: E1) = bind2(bind1(v1))
    }

  implicit def BindTuple3[
    V1<:VariableExpr[V1],V2<:VariableExpr[V2],V3<:VariableExpr[V3],T1,T2,T3,
    E1<:VariableExpr[E1],E2<:VariableExpr[E2],E3<:VariableExpr[E3],E4<:VariableExpr[E4]
  ](implicit bind1: Binder[V1,T1,E1,E2],
    bind2: Binder[V2,T2,E2,E3],
    bind3: Binder[V3,T3,E3,E4]): Binder[Tuple3VariableExpr[V1,V2,V3],(T1,T2,T3),E1,E4] =
    new Binder[Tuple3VariableExpr[V1,V2,V3],(T1,T2,T3),E1,E4] {
      def apply(v1: E1) = bind3(bind2(bind1(v1)))
    }
}

trait StandardPriorityBindingImplicits extends LowPriorityBindingImplicits {
  /** Binding base cases - primitive expressions don't need to bind anything, variables bind iff they are untyped variables
    * matching the type in the binder. (otherwise they use the low priority non-binding case)
    */
    implicit def PrimitiveExprBinder[V <: VariableExpr[V],T,E <: PrimitiveExpr[_]]: Binder[V,T,E,E] =
      Binder.nonBinder[V,T,E]

//  implicit def IntExprBinder[V <: VariableExpr[V],T]: Binder[V,T,IntExpr,IntExpr] = Binder.nonBinder[V,T,IntExpr]
//
//  implicit def PhysicalCollectionBinder[V <: VariableExpr[V],T,C[_,_],K,R]:
//  Binder[V,T,PhysicalCollection[C,K,R],PhysicalCollection[C,K,R]] = Binder.nonBinder[V,T,PhysicalCollection[C,K,R]]

  implicit def VariableBinder[V <: UntypedVariable[V],T,V1 <: UntypedVariable[V1]](implicit ev: V1 <:< V): Binder[V,T,V1,TypedVariable[T]] =
    new Binder[V,T,V1,TypedVariable[T]] { def apply(v1: V1): TypedVariable[T] = v1.tag[T] }

  /**Special binder for inner infinite mappings - dont attempt to bind the key*/
  implicit def InfMappingBinder[V <: VariableExpr[V],T,K <: VariableExpr[K],R <: RingExpr,R1 <: RingExpr]
  (implicit bindR: Binder[V,T,R,R1]): Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] =
    new Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] {
      def apply(v1: InfiniteMappingExpr[K,R]) = InfiniteMappingExpr(v1.key,bindR(v1.value))
    }


  /**Standard inductive binder cases.*/
  //This applies a single binder to a Tuple2 variable expression.
  implicit def Tuple2Binder[
    V <: VariableExpr[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2],
    V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B]
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B]):
    Binder[V,T,Tuple2VariableExpr[V1,V2],Tuple2VariableExpr[V1B,V2B]] =
      new Binder[V,T,Tuple2VariableExpr[V1,V2],Tuple2VariableExpr[V1B,V2B]] {
        def apply(v1: Tuple2VariableExpr[V1,V2]) = Tuple2VariableExpr(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def Tuple3Binder[
    V <: VariableExpr[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], V3 <: VariableExpr[V3],
    V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B], V3B <: VariableExpr[V3B]
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B], bind3: Binder[V,T,V3,V3B]):
    Binder[V,T,Tuple3VariableExpr[V1,V2,V3],Tuple3VariableExpr[V1B,V2B,V3B]] =
      new Binder[V,T,Tuple3VariableExpr[V1,V2,V3],Tuple3VariableExpr[V1B,V2B,V3B]] {
      def apply(v1: Tuple3VariableExpr[V1,V2,V3]) = Tuple3VariableExpr(bind1(v1.c1), bind2(v1.c2), bind3(v1.c3))
    }

  implicit def SumBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,SumExpr[R],SumExpr[R1]] =
    new Binder[V,T,SumExpr[R],SumExpr[R1]] { def apply(v1: SumExpr[R]) = SumExpr(recur(v1.c1)) }

  implicit def SngBinder[V <: VariableExpr[V],T,K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit bindK: Binder[V,T,K,K1], bindR: Binder[V,T,R,R1]): Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] =
    new Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(bindK(v1.key),bindR(v1.value)) }

  implicit def MultiplyBinder[V <: VariableExpr[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] { def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(bindL(v1.c1),bindR(v1.c2)) }

  implicit def DotBinder[V <: VariableExpr[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,DotExpr[L,R],DotExpr[L1,R1]] =
    new Binder[V,T,DotExpr[L,R],DotExpr[L1,R1]] { def apply(v1: DotExpr[L,R]) = DotExpr(bindL(v1.c1),bindR(v1.c2)) }

  implicit def ToRingBinder[V <: VariableExpr[V],T,R <: Expr,R1 <: Expr]
  (implicit bind: Binder[V,T,R,R1]): Binder[V,T,ToRingExpr[R],ToRingExpr[R1]] =
    new Binder[V,T,ToRingExpr[R],ToRingExpr[R1]] { def apply(v1: ToRingExpr[R]) = ToRingExpr(bind(v1.c1)) }

  implicit def BoxedRingBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit bind: Binder[V,T,R,R1]): Binder[V,T,BoxedRingExpr[R],BoxedRingExpr[R1]] =
    new Binder[V,T,BoxedRingExpr[R],BoxedRingExpr[R1]] {
      def apply(v1: BoxedRingExpr[R]) = BoxedRingExpr(bind(v1.c1))
    }

  implicit def Tuple2KeyBinder[V <: VariableExpr[V],T,K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B]): Binder[V,T,Tuple2KeyExpr[K1,K2],Tuple2KeyExpr[K1B,K2B]] =
    new Binder[V,T,Tuple2KeyExpr[K1,K2],Tuple2KeyExpr[K1B,K2B]] {
      def apply(v1: Tuple2KeyExpr[K1,K2]) = Tuple2KeyExpr(bind1(v1.c1),bind2(v1.c2))
    }

  implicit def Tuple3KeyBinder[V <: VariableExpr[V],T,K1 <: KeyExpr,K2 <: KeyExpr,K3 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr,K3B <: KeyExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B], bind3: Binder[V,T,K3,K3B]): Binder[V,T,Tuple3KeyExpr[K1,K2,K3],Tuple3KeyExpr[K1B,K2B,K3B]] =
    new Binder[V,T,Tuple3KeyExpr[K1,K2,K3],Tuple3KeyExpr[K1B,K2B,K3B]] {
      def apply(v1: Tuple3KeyExpr[K1,K2,K3]) = Tuple3KeyExpr(bind1(v1.c1),bind2(v1.c2),bind3(v1.c3))
    }

  implicit def EqualsPredicateBinder[V <: VariableExpr[V],T,K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B]): Binder[V,T,EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] =
    new Binder[V,T,EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] {
      def apply(v1: EqualsPredicate[K1,K2]) = EqualsPredicate(bind1(v1.c1),bind2(v1.c2))
    }
}

trait HighPriorityResolutionImplicits extends StandardPriorityResolutionImplicits {
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
    LHS <: RingExpr, LHS1 <: RingExpr, V <: VariableExpr[V], C[_,_], KT, VB <: VariableExpr[VB],
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

trait HighPriorityBindingImplicits extends StandardPriorityBindingImplicits {
  //Never try to find an actual binder for a typed variable.
  implicit def TypedNonBinder[V <: VariableExpr[V],V1 <: TypedVariable[_],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]
}

trait VariableResolutionImplicits extends HighPriorityBindingImplicits with HighPriorityResolutionImplicits

//
//
//object test {
//
//  import implicits._
//
//  def main(args: Array[String]): Unit = {
//    def getTag[T](implicit ev1: ClassTag[T]): ClassTag[T] = ev1
//    def getType[T](t: T)(implicit ev1: TypeTag[T]): TypeTag[T] = ev1
//    val bagOfInts = PhysicalCollection(Map(1 -> 1))
//    val bagOfIntPairs = PhysicalCollection(Map((1,2) -> 1))
//    val unevenTuples = PhysicalCollection(Map(((1,2),3) -> 1))
//
//    val query = For (X <-- bagOfInts) Collect IntExpr(2)
//      println(query.eval)
////    println()
////    println(query.eval)
////    val expr = For ((X,bagOfInts)) Collect IntExpr(1)
////    println(expr.eval)
////    def go[V <: VariableExpr[V]](v: V) = println("hi")
////    go(X)
//
//
//    //val tuple2maker = tuple2MakeVariableExpr[X,Y,X,Y]//(idMakeExpr[X],idMakeExpr[Y])
//
////    val expr2 = For ( Tuple2VariableExpr(X,Y) <-- bagOfIntPairs) Yield toExpr[(X,Y),Tuple2VariableExpr[X,Y]]((X,Y))
////    println(expr2.eval)
////    val expr3 = For (X <-- bagOfInts) Collect ( For (Y <-- bagOfInts) Yield X )
////    println(expr3.eval)
//
//  }
//}

