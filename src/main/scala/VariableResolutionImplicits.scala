package slender

import shapeless._

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
trait ResolverBase[-In,+Out] extends (In => Out) with Serializable

trait Resolver[-In,+Out] extends ResolverBase[In,Out]

trait Binder[V <: VariableExpr[V],T,-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

object Resolver {

  def instance[In, Out](f: In => Out): Resolver[In,Out] = new Resolver[In,Out] {
    def apply(v1: In): Out = f(v1)
  }

  def nonResolver[E <: Expr]: Resolver[E,E] = new Resolver[E,E] { def apply(v1: E) = v1 }
}

object Binder {
  def nonBinder[V <: VariableExpr[V], T, E <: Expr]: Binder[V,T,E,E] =
    new Binder[V,T,E,E] { def apply(v1: E) = v1 }
}


trait Priority0ResolutionImplicits {
  //A default ResolverBase for any expression, so that for-comprehensions in the DSL can resolve expressions before
  //returning them (inner expressions may not be resolvable when initially returned).
  //Note that the other methods here and the main Resolve method require a Resolver not a ResolverBase so none of
  //those can use this one.
  implicit def CantResolve[E <: Expr]: ResolverBase[E,E] = new ResolverBase[E,E] { def apply(v1: E) = v1 }
}

trait Priority01ResolutionImplicits extends Priority0ResolutionImplicits {

//  implicit def HNilResolver: Resolver[GenExpr[HNil], GenExpr[HNil]] =
//    Resolver.instance[GenExpr[HNil], GenExpr[HNil]](identity[GenExpr[HNil]])
//
//  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
//  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[GenExpr[T1], GenExpr[T2]]):
//    Resolver[GenExpr[H1 :: T1], GenExpr[H2 :: T2]] = Resolver.instance[GenExpr[H1 :: T1], GenExpr[H2 :: T2]] {
//    case GenExpr(h1 :: t1) => {
//      val h2 = resolveH(h1)
//      val t2 = resolveT(GenExpr(t1)).repr
//      val hlist2 = h2 :: t2
//      GenExpr(hlist2)
//    }
//  }
  implicit def HNilResolver: Resolver[HNil,HNil] =
    Resolver.instance(identity[HNil])

  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[T1, T2]):
    Resolver[H1 :: T1, H2 :: T2] = Resolver.instance[H1 :: T1, H2 :: T2] {
    case h1 :: t1 => resolveH(h1) :: resolveT(t1)
  }

  implicit def InductiveResolver2[
    C[_ <: Expr,_ <: Expr],L1 <: Expr,R1 <: Expr,L2 <: Expr,R2 <: Expr
  ]
  (implicit gen1: Generic.Aux[C[L1,R1],L1::R1::HNil], genResolve: Resolver[L1::R1::HNil,L2::R2::HNil],
   gen2: Generic.Aux[C[L2,R2], L2::R2::HNil]):
  Resolver[C[L1,R1], C[L2,R2]] = new Resolver[C[L1,R1], C[L2,R2]] {
    def apply(v1: C[L1,R1]): C[L2, R2] = gen2.from(genResolve(gen1.to(v1)))
  }

  implicit def InductiveResolver1[
    C[_ <: RingExpr],E1 <: RingExpr,E2 <: RingExpr
  ]
  (implicit gen1: Generic.Aux[C[E1],E1::HNil], genResolve: Resolver[E1::HNil,E2::HNil],
   gen2: Generic.Aux[C[E2], E2::HNil]):
  Resolver[C[E1], C[E2]] = new Resolver[C[E1], C[E2]] {
    def apply(v1: C[E1]): C[E2] = gen2.from(genResolve(gen1.to(v1)))
  }

//  implicit def InductiveResolver2[
//  C[_,_],L1 <: Expr,R1 <: Expr,L2 <: Expr,R2 <: Expr, Repr1 <: HList, Repr2 <: HList
//  ]
//  (implicit gen1: Generic.Aux[C[L1,R1],Repr1], genResolve: Resolver[Repr1,Repr2], gen2: Generic.Aux[C[L2,R2], Repr2]):
//  Resolver[C[L1,R1], C[L2,R2]] = new Resolver[C[L1,R1], C[L2,R2]] {
//    def apply(v1: C[L1,R1]) = gen2.from(genResolve(gen1.to(v1)))
//  }
}

trait Priority1ResolutionImplicits extends Priority01ResolutionImplicits {
  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def PrimitiveExprResolver[E <: PrimitiveExpr[_]]: Resolver[E,E] = Resolver.nonResolver[E]

  implicit def VariableResolver[V <: UntypedVariable[V],T]: Resolver[TypedVariable[T],TypedVariable[T]] =
    Resolver.nonResolver[TypedVariable[T]]

//
//  implicit def Project1RingResolver[R1 <: RingExpr with C1Expr,R2 <: RingExpr with C1Expr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project1RingExpr[R1],Project1RingExpr[R2]] = new Resolver[Project1RingExpr[R1],Project1RingExpr[R2]] {
//    def apply(v1: Project1RingExpr[R1]): Project1RingExpr[R2] = Project1RingExpr(resolver(v1.c1))
//  }
//
//  implicit def Project1KeyResolver[R1 <: KeyExpr,R2 <: KeyExpr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project1KeyExpr[R1],Project1KeyExpr[R2]] = new Resolver[Project1KeyExpr[R1],Project1KeyExpr[R2]] {
//    def apply(v1: Project1KeyExpr[R1]): Project1KeyExpr[R2] = Project1KeyExpr(resolver(v1.c1))
//  }
//
//  implicit def Project2RingResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project2RingExpr[R1],Project2RingExpr[R2]] = new Resolver[Project2RingExpr[R1],Project2RingExpr[R2]] {
//    def apply(v1: Project2RingExpr[R1]): Project2RingExpr[R2] = Project2RingExpr(resolver(v1.c1))
//  }
//
//  implicit def Project2KeyResolver[R1 <: KeyExpr,R2 <: KeyExpr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project2KeyExpr[R1],Project2KeyExpr[R2]] = new Resolver[Project2KeyExpr[R1],Project2KeyExpr[R2]] {
//    def apply(v1: Project2KeyExpr[R1]): Project2KeyExpr[R2] = Project2KeyExpr(resolver(v1.c1))
//  }
//
//  implicit def Project3RingResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project3RingExpr[R1],Project3RingExpr[R2]] = new Resolver[Project3RingExpr[R1],Project3RingExpr[R2]] {
//    def apply(v1: Project3RingExpr[R1]): Project3RingExpr[R2] = Project3RingExpr(resolver(v1.c1))
//  }
//
//  implicit def Project3KeyResolver[R1 <: KeyExpr,R2 <: KeyExpr](implicit resolver: Resolver[R1,R2]):
//  Resolver[Project3KeyExpr[R1],Project3KeyExpr[R2]] = new Resolver[Project3KeyExpr[R1],Project3KeyExpr[R2]] {
//    def apply(v1: Project3KeyExpr[R1]): Project3KeyExpr[R2] = Project3KeyExpr(resolver(v1.c1))
//  }
}


trait Priority2ResolutionImplicits extends Priority1ResolutionImplicits {
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


trait LowPriorityBindingImplicits {
  //The case that V1 <:< V and so actually binds to T takes precedence, hence this non-binder is here.
  implicit def VariableNonBinder[V <: UntypedVariable[V],V1 <: UntypedVariable[V1],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]

  //The standard (non-transitive) reconstructive methods for tuples take precedence so that there is no ambiguity in the specific
  //case that we request a Binder for a tupled variable expression to apply to a tupled varaible expression, and so either route will work (though
  //both then delegate to the other in symmetry).
  //Also it seems likely that the transitivity puts a lot of strain on the compiler so the lower we push such a search down the tree
  //the easier we make it for the compiler.
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

  implicit def VariableBinder[V <: UntypedVariable[V],T,V1 <: UntypedVariable[V1]](implicit ev: V1 <:< V): Binder[V,T,V1,TypedVariable[T]] =
    new Binder[V,T,V1,TypedVariable[T]] { def apply(v1: V1): TypedVariable[T] = v1.tag[T] }

  /**Special binder for inner infinite mappings - dont attempt to bind the key*/
  implicit def InfMappingBinder[V <: VariableExpr[V],T,K <: VariableExpr[K],R <: RingExpr,R1 <: RingExpr]
  (implicit bindR: Binder[V,T,R,R1]): Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] =
    new Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] {
      def apply(v1: InfiniteMappingExpr[K,R]) = InfiniteMappingExpr(v1.key,bindR(v1.value))
    }


  /**Standard inductive binder cases.*/
  implicit def AddBinder[V <: VariableExpr[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,AddExpr[L,R],AddExpr[L1,R1]] =
    new Binder[V,T,AddExpr[L,R],AddExpr[L1,R1]] { def apply(v1: AddExpr[L,R]) = AddExpr(bindL(v1.c1),bindR(v1.c2)) }

  implicit def MultiplyBinder[V <: VariableExpr[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] { def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(bindL(v1.c1),bindR(v1.c2)) }

  implicit def DotBinder[V <: VariableExpr[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,DotExpr[L,R],DotExpr[L1,R1]] =
    new Binder[V,T,DotExpr[L,R],DotExpr[L1,R1]] { def apply(v1: DotExpr[L,R]) = DotExpr(bindL(v1.c1),bindR(v1.c2)) }
  //This applies a single binder to a Tuple2 variable expression.

  implicit def NotBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,NotExpr[R],NotExpr[R1]] =
    new Binder[V,T,NotExpr[R],NotExpr[R1]] { def apply(v1: NotExpr[R]) = NotExpr(recur(v1.c1)) }

  implicit def NegateBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,NegateExpr[R],NegateExpr[R1]] =
    new Binder[V,T,NegateExpr[R],NegateExpr[R1]] { def apply(v1: NegateExpr[R]) = NegateExpr(recur(v1.c1)) }

  implicit def SumBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,SumExpr[R],SumExpr[R1]] =
    new Binder[V,T,SumExpr[R],SumExpr[R1]] { def apply(v1: SumExpr[R]) = SumExpr(recur(v1.c1)) }

  implicit def SngBinder[V <: VariableExpr[V],T,K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit bindK: Binder[V,T,K,K1], bindR: Binder[V,T,R,R1]): Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] =
    new Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(bindK(v1.key),bindR(v1.value)) }

  implicit def EqualsPredicateBinder[V <: VariableExpr[V],T,K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B]): Binder[V,T,EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] =
    new Binder[V,T,EqualsPredicate[K1,K2],EqualsPredicate[K1B,K2B]] {
      def apply(v1: EqualsPredicate[K1,K2]) = EqualsPredicate(bind1(v1.c1),bind2(v1.c2))
    }

  implicit def IntPredicateBinder[V <: VariableExpr[V],T,K1 <: KeyExpr,K2 <: KeyExpr,K1B <: KeyExpr,K2B <: KeyExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B]): Binder[V,T,IntPredicate[K1,K2],IntPredicate[K1B,K2B]] =
    new Binder[V,T,IntPredicate[K1,K2],IntPredicate[K1B,K2B]] {
      def apply(v1: IntPredicate[K1,K2]) = IntPredicate(bind1(v1.c1),bind2(v1.c2), v1.p, v1.opString)
    }

  implicit def ToRingBinder[V <: VariableExpr[V],T,R <: Expr,R1 <: Expr]
  (implicit bind: Binder[V,T,R,R1]): Binder[V,T,ToRingExpr[R],ToRingExpr[R1]] =
    new Binder[V,T,ToRingExpr[R],ToRingExpr[R1]] { def apply(v1: ToRingExpr[R]) = ToRingExpr(bind(v1.c1)) }

  implicit def BoxedRingBinder[V <: VariableExpr[V],T,R <: Expr,R1 <: RingExpr]
  (implicit bind: Binder[V,T,R,R1]): Binder[V,T,BoxedRingExpr[R],BoxedRingExpr[R1]] =
    new Binder[V,T,BoxedRingExpr[R],BoxedRingExpr[R1]] { def apply(v1: BoxedRingExpr[R]) = BoxedRingExpr(bind(v1.c1)) }

  implicit def LabelBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit bind: Binder[V,T,R,R1]): Binder[V,T,LabelExpr[R],LabelExpr[R1]] =
    new Binder[V,T,LabelExpr[R],LabelExpr[R1]] { def apply(v1: LabelExpr[R]) = LabelExpr(bind(v1.c1)) }

  //todo - do these need to be higher priority than the key binder methods? and would the key methods need to be lower prioirty
  //than the transitive methods?
  implicit def Tuple2VariableBinder[
    V <: VariableExpr[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2],
    V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B]
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B]):
    Binder[V,T,Tuple2VariableExpr[V1,V2],Tuple2VariableExpr[V1B,V2B]] =
      new Binder[V,T,Tuple2VariableExpr[V1,V2],Tuple2VariableExpr[V1B,V2B]] {
        def apply(v1: Tuple2VariableExpr[V1,V2]) = Tuple2VariableExpr(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def Tuple3VariableBinder[
    V <: VariableExpr[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], V3 <: VariableExpr[V3],
    V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B], V3B <: VariableExpr[V3B]
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B], bind3: Binder[V,T,V3,V3B]):
    Binder[V,T,Tuple3VariableExpr[V1,V2,V3],Tuple3VariableExpr[V1B,V2B,V3B]] =
      new Binder[V,T,Tuple3VariableExpr[V1,V2,V3],Tuple3VariableExpr[V1B,V2B,V3B]] {
      def apply(v1: Tuple3VariableExpr[V1,V2,V3]) = Tuple3VariableExpr(bind1(v1.c1), bind2(v1.c2), bind3(v1.c3))
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

  implicit def Tuple2RingBinder[V <: VariableExpr[V],T,K1 <: RingExpr,K2 <: RingExpr,K1B <: RingExpr,K2B <: RingExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B]): Binder[V,T,Tuple2RingExpr[K1,K2],Tuple2RingExpr[K1B,K2B]] =
    new Binder[V,T,Tuple2RingExpr[K1,K2],Tuple2RingExpr[K1B,K2B]] {
      def apply(v1: Tuple2RingExpr[K1,K2]) = Tuple2RingExpr(bind1(v1.c1),bind2(v1.c2))
    }

  implicit def Tuple3RingBinder[V <: VariableExpr[V],T,K1 <: RingExpr,K2 <: RingExpr,K3 <: RingExpr,K1B <: RingExpr,K2B <: RingExpr,K3B <: RingExpr]
  (implicit bind1: Binder[V,T,K1,K1B], bind2: Binder[V,T,K2,K2B], bind3: Binder[V,T,K3,K3B]): Binder[V,T,Tuple3RingExpr[K1,K2,K3],Tuple3RingExpr[K1B,K2B,K3B]] =
    new Binder[V,T,Tuple3RingExpr[K1,K2,K3],Tuple3RingExpr[K1B,K2B,K3B]] {
      def apply(v1: Tuple3RingExpr[K1,K2,K3]) = Tuple3RingExpr(bind1(v1.c1),bind2(v1.c2),bind3(v1.c3))
    }

//  implicit def Project1RingBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
//  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,Project1RingExpr[R],Project1RingExpr[R1]] =
//    new Binder[V,T,Project1RingExpr[R],Project1RingExpr[R1]] { def apply(v1: Project1RingExpr[R]) = Project1RingExpr(recur(v1.c1)) }
//
//  implicit def Project2RingBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
//  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,Project2RingExpr[R],Project2RingExpr[R1]] =
//    new Binder[V,T,Project2RingExpr[R],Project2RingExpr[R1]] { def apply(v1: Project2RingExpr[R]) = Project2RingExpr(recur(v1.c1)) }
//
//  implicit def Project3RingBinder[V <: VariableExpr[V],T,R <: RingExpr,R1 <: RingExpr]
//  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,Project3RingExpr[R],Project3RingExpr[R1]] =
//    new Binder[V,T,Project3RingExpr[R],Project3RingExpr[R1]] { def apply(v1: Project3RingExpr[R]) = Project3RingExpr(recur(v1.c1)) }
//
//  implicit def Project1KeyBinder[V <: VariableExpr[V],T,R <: KeyExpr,R1 <: KeyExpr]
//  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,Project1KeyExpr[R],Project1KeyExpr[R1]] =
//    new Binder[V,T,Project1KeyExpr[R],Project1KeyExpr[R1]] { def apply(v1: Project1KeyExpr[R]) = Project1KeyExpr(recur(v1.c1)) }
//
//  implicit def Project2KeyBinder[V <: VariableExpr[V],T,R <: KeyExpr,R1 <: KeyExpr]
//  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,Project2KeyExpr[R],Project2KeyExpr[R1]] =
//    new Binder[V,T,Project2KeyExpr[R],Project2KeyExpr[R1]] { def apply(v1: Project2KeyExpr[R]) = Project2KeyExpr(recur(v1.c1)) }
//
//  implicit def Project3KeyBinder[V <: VariableExpr[V],T,R <: KeyExpr,R1 <: KeyExpr]
//  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,Project3KeyExpr[R],Project3KeyExpr[R1]] =
//    new Binder[V,T,Project3KeyExpr[R],Project3KeyExpr[R1]] { def apply(v1: Project3KeyExpr[R]) = Project3KeyExpr(recur(v1.c1)) }

}

trait HighPriorityBindingImplicits extends StandardPriorityBindingImplicits {
  //Never try to find an actual binder for a typed variable.
  implicit def TypedNonBinder[V <: VariableExpr[V],V1 <: TypedVariable[_],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]
}

trait VariableResolutionImplicits extends HighPriorityBindingImplicits with Priority2ResolutionImplicits