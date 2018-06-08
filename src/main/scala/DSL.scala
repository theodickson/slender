package slender

trait MakeExpr[X,E <: Expr] extends (X => E)

trait ExtraLowPriorityDSL {
  implicit def toKMakeExpr[T,R <: RingExpr](implicit recur: MakeExpr[T,R]): MakeExpr[T,BoxedRingExpr[R]] =
    new MakeExpr[T,BoxedRingExpr[R]] {
      def apply(v1: T) = BoxedRingExpr(recur(v1))
    }
}

trait LowPriorityDSL extends ExtraLowPriorityDSL {

  implicit def toExpr[X,E <: Expr](x: X)(implicit make: MakeExpr[X,E]): E = make(x)

  implicit def numericMakeRing[N : Numeric]: MakeExpr[N,NumericExpr[N]] =
    new MakeExpr[N,NumericExpr[N]] { def apply(v1: N) = NumericExpr(v1) }

  implicit def idMakeExpr[E <: Expr]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }

  implicit def tuple2MakeRingExpr[X1,X2,R1 <: RingExpr, R2 <: RingExpr]
    (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2RingExpr(recur1(v1._1),recur2(v1._2))
    }

  implicit def tuple3MakeRingExpr[X1,X2,X3,R1 <: RingExpr, R2 <: RingExpr, R3 <: RingExpr]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
  MakeExpr[(X1,X2,X3),Tuple3RingExpr[R1,R2,R3]] =
    new MakeExpr[(X1,X2,X3),Tuple3RingExpr[R1,R2,R3]] {
      def apply(v1: (X1,X2,X3)) = Tuple3RingExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
    }

  implicit def tuple2MakeKeyExpr[X1,X2,R1 <: KeyExpr, R2 <: KeyExpr]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2KeyExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2KeyExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2KeyExpr(recur1(v1._1),recur2(v1._2))
    }

  implicit def tuple3MakeKeyExpr[X1,X2,X3,R1 <: KeyExpr, R2 <: KeyExpr, R3 <: KeyExpr]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
  MakeExpr[(X1,X2,X3),Tuple3KeyExpr[R1,R2,R3]] =
    new MakeExpr[(X1,X2,X3),Tuple3KeyExpr[R1,R2,R3]] {
      def apply(v1: (X1,X2,X3)) = Tuple3KeyExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
    }


  def sum[R <: RingExpr](r: R): SumExpr[R] = SumExpr(r)

  def sng[K <: KeyExpr, R <: RingExpr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  def sng[K <: KeyExpr](k: K): SngExpr[K, NumericExpr[Int]] = SngExpr(k, NumericExpr(1))

  def toK[E <: RingExpr](e: E): BoxedRingExpr[E] = BoxedRingExpr(e)

  def toRing[E <: Expr](e: E): ToRingExpr[E] = ToRingExpr(e)


  trait MakeKeyRingPair[X,K <: KeyExpr,R <: RingExpr] extends (X => KeyRingPair[K,R])

  implicit def IdMakeKeyRingPair[K <: KeyExpr, R <: RingExpr]: MakeKeyRingPair[KeyRingPair[K,R],K,R] =
    new MakeKeyRingPair[KeyRingPair[K,R],K,R] { def apply(v1: KeyRingPair[K,R]): KeyRingPair[K,R] = v1 }

  implicit def ImplicitOne[X, K <: KeyExpr](implicit make: MakeExpr[X,K]): MakeKeyRingPair[X,K,NumericExpr[Int]] =
    new MakeKeyRingPair[X,K,NumericExpr[Int]] {
      def apply(v1: X): KeyRingPair[K,NumericExpr[Int]] = KeyRingPair(make(v1),NumericExpr(1))
    }

//  case class ForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr](x: V, r1: R) {
//
//    def _collect[R2 <: RingExpr, Out <: Expr]
//      (r2: R2)
//      (implicit resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]],Out]): Out =
//        resolver(SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, r2))))
//
//    def Collect[T, R2 <: RingExpr, Out <: Expr]
//      (r2: T)
//      (implicit make: MakeExpr[T,R2], resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]],Out]) =
//      _collect(make(r2))
//
//    def Yield[T, K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
//    (x: T)
//    (implicit make: MakeKeyRingPair[T,K,R2],
//     resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, R2]]]], Out]): Out = {
//      val made = make(x)
//      _collect(SngExpr(made.k, made.r))
//    }
//
//  }

//  case class PredicatedForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr]
//  (x: V, r1: R, p: P = NumericExpr(1)) {
//
//    val builder = ForComprehensionBuilder(x,r1)
//
//    def Collect[T, R2 <: RingExpr, Out <: Expr]
//    (r2: T)
//    (implicit make: MakeExpr[T,R2],
//     resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]], Out]) =
//      builder._collect(DotExpr[R2,P](make(r2),p))
//
//    def Yield[T, K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
//    (k: T)
//    (implicit make: MakeKeyRingPair[T,K,R2],
//     resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, DotExpr[R2,P]]]]], Out]): Out = {
//      val made = make(k)
//      builder._collect(SngExpr(made.k,DotExpr(made.r, p)))
//    }
//  }
  case class ForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](vrp: VariableRingPredicate[V,R,P]) {
    //todo - get rid of default NumericExpr(1)
//    def _collect[R2 <: RingExpr, Out <: Expr]
//    (r2: R2)
//    (implicit resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]],Out]): Out =
//      resolver(SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, DotExpr(r2,p)))))

    val x = vrp.k; val r1 = vrp.r; val p = vrp.p

    def _collect[R2 <: RingExpr](r2: R2): SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]] =
      SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, DotExpr(r2,p))))

    def Collect[T, R2 <: RingExpr, Out <: Expr]
      (r2: T)
      (implicit make: MakeExpr[T,R2], resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]],Out]) =
        resolver(_collect(make(r2)))

    def Yield[T, K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
      (x: T)
      (implicit make: MakeKeyRingPair[T,K,R2],
       resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[SngExpr[K, R2],P]]]], Out]): Out = {
        val made = make(x)
        resolver(_collect(SngExpr(made.k, made.r)))
      }
  }

  case class NestedForComprehensionBuilder[
    V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], R1 <: RingExpr, R2 <: RingExpr, P1 <: RingExpr, P2 <: RingExpr
  ](builder1: ForComprehensionBuilder[V1,R1,P1], builder2: ForComprehensionBuilder[V2,R2,P2]) {

    def Collect[T, R3 <: RingExpr, Out <: Expr]
    (r3: T)(implicit make: MakeExpr[T, R3],
            resolver: ResolverBase[
              SumExpr[
                MultiplyExpr[
                  R1,
                  InfiniteMappingExpr[
                    V1,
                    DotExpr[SumExpr[MultiplyExpr[R2, InfiniteMappingExpr[V2, DotExpr[R3, P2]]]], P1]]]],
              Out
              ]) = {
      val made = make(r3)
      builder1._collect(builder2._collect(made))
    }

    def Yield[T, K <: KeyExpr, R3 <: RingExpr, Out <: Expr]
    (r3: T)(implicit make: MakeKeyRingPair[T, K, R3],
            resolver: ResolverBase[
              SumExpr[
                MultiplyExpr[
                  R1,
                  InfiniteMappingExpr[
                    V1,
                    DotExpr[SumExpr[MultiplyExpr[R2, InfiniteMappingExpr[V2, DotExpr[SngExpr[K, R3], P2]]]], P1]]]],
              Out
              ]) = {
      val made = make(r3)
      builder1._collect(builder2._collect(SngExpr(made.k, made.r)))
    }
  }



//  case class ForComprehensionBuilder2[V1 <: VariableExpr[V1], R1 <: RingExpr, V2 <: VariableExpr[V2], R2 <: RingExpr]
//    (x1: V1, r1: R1, x2: V2, r2: R2) {
//
//    def _collect[R3 <: RingExpr, Out <: Expr]
//    (r3: R3)
//    (implicit resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]],Out]): Out =
//      For (x1 <-- r1) Collect (For (x2 <-- r2) Collect r3)
//      resolver(SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, r2))))
//
//    def Collect[T, R2 <: RingExpr, Out <: Expr]
//    (r2: T)
//    (implicit make: MakeExpr[T,R2], resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]],Out]) =
//      _collect(make(r2))
//
//    def Yield[T, K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
//    (x: T)
//    (implicit make: MakeKeyRingPair[T,K,R2],
//     resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, R2]]]], Out]): Out = {
//      val made = make(x)
//      _collect(SngExpr(made.k, made.r))
//    }
//
//  }

  object For {
    def apply[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](vrp: VariableRingPredicate[V,R,P]):
      ForComprehensionBuilder[V, R, P] = ForComprehensionBuilder(vrp)

    def apply[
      V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], R1 <: RingExpr, R2 <: RingExpr, P1 <: RingExpr, P2 <: RingExpr
    ](vrp1: VariableRingPredicate[V1,R1,P1],
      vrp2: VariableRingPredicate[V2,R2,P2]): NestedForComprehensionBuilder[V1, V2, R1, R2, P1, P2] = {
      NestedForComprehensionBuilder(ForComprehensionBuilder(vrp1), ForComprehensionBuilder(vrp2))
    }
  }
//
//    def apply[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](args: (V,R,P)):
//      ForComprehensionBuilder[V,R,P] = ForComprehensionBuilder[V,R,P](args._1, args._2, args._3)

//    def apply

  implicit class IffImplicit[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](pair: VariableRingPredicate[V,R,P]) {
    def iff[P2 <: RingExpr](p: P2): VariableRingPredicate[V,R,P2] = VariableRingPredicate(pair.k,pair.r,p)
  }

}

trait DSL extends LowPriorityDSL {

  //It's higher priority to make tuples of only variables into tuple variable expressions so that expressions
  //can be resolved. It also means they have access to the <-- method needed to make For comprehensions look nice.
  implicit def tuple2MakeVariableExpr[X1,X2,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2VariableExpr(recur1(v1._1),recur2(v1._2))
    }

  implicit def tuple3MakeVariableExpr[X1,X2,X3,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2], R3 <: VariableExpr[R3]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
  MakeExpr[(X1,X2,X3),Tuple3VariableExpr[R1,R2,R3]] =
    new MakeExpr[(X1,X2,X3),Tuple3VariableExpr[R1,R2,R3]] {
      def apply(v1: (X1,X2,X3)) = Tuple3VariableExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
    }

}