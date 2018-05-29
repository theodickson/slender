package slender

trait LowPriorityDSL {

  trait MakeExpr[X,E <: Expr] extends (X => E)

  implicit def toExpr[X,E <: Expr](x: X)(implicit make: MakeExpr[X,E]): E = make(x)

  implicit def intMakeRing: MakeExpr[Int,IntExpr] = new MakeExpr[Int,IntExpr] { def apply(v1: Int) = IntExpr(v1) }

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


  implicit def sng[K <: KeyExpr, R <: RingExpr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  implicit def sng[K <: KeyExpr](k: K): SngExpr[K, IntExpr] = SngExpr(k, IntExpr(1))

  def toK[E <: RingExpr](e: E): BoxedRingExpr[E] = BoxedRingExpr(e)

  def toRing[E <: Expr](e: E): ToRingExpr[E] = ToRingExpr(e)

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)


  case class ForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr](x: V, r1: R) {

    def _collect[R2 <: RingExpr, Out <: Expr]
      (r2: R2)
      (implicit resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]],Out]): Out =
        resolver(SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, r2))))

    def Collect[T, R2 <: RingExpr, Out <: Expr]
      (r2: T)
      (implicit make: MakeExpr[T,R2], resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]],Out]) =
      _collect(make(r2))

//    def YieldSng[K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
//      (r2: SngExpr[K,R2])
//      (implicit resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, R2]]]], Out]): Out =
//        Collect(r2)
//
    def Yield[T, K <: KeyExpr, Out <: Expr]
      (k: T)
      (implicit make: MakeExpr[T,K],
       resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, IntExpr]]]], Out]): Out =
        _collect(SngExpr(make(k), IntExpr(1)))

    //todo - yield a k->v without a differently named method.

//    def YieldSng[T, K <: KeyExpr, R <: RingExpr, Out <: Expr]
//      (k: T)
//      (implicit make: MakeExpr[T,SngExpr[K,R]],
//        resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, IntExpr]]]], Out]): Out =
//          _collect(make(k))

//    def CollectUn[R2 <: RingExpr](r2: R2): SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]] =
//      SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, r2)))
//
//    def YieldSngUn[K <: KeyExpr, R2 <: RingExpr](r2: SngExpr[K,R2]):
//      SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, R2]]]] = CollectUn(r2)
//
//    def YieldUn[K <: KeyExpr](k: K): SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, IntExpr]]]] =
//      CollectUn(SngExpr(k, IntExpr(1)))

  }

  case class PredicatedForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](x: V, r1: R, p: P) {

    val builder = ForComprehensionBuilder(x,r1)

    def Collect[T, R2 <: RingExpr, Out <: Expr]
    (r2: T)
    (implicit make: MakeExpr[T,R2],
              resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]], Out]) =
      builder._collect(DotExpr[R2,P](make(r2),p))

//    def Yield[K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
//    (pair: KeyRingPair[K, R2])
//    (implicit resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, DotExpr[R2,P]]]]], Out]): Out =
//      builder.Yield(KeyRingPair(pair.k,DotExpr[R2,P](pair.r,p)))
    def Yield[T, K <: KeyExpr, Out <: Expr]
    (k: T)
    (implicit make: MakeExpr[T,K], resolver: ResolverBase[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, P]]]], Out]): Out =
      builder._collect(SngExpr(make(k),p))

  }

  object For {
    def apply[V <: VariableExpr[V], R <: RingExpr](pair: VariableRingPair[V, R]): ForComprehensionBuilder[V, R] =
      ForComprehensionBuilder[V, R](pair.k, pair.r)

    def apply[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](args: (V,R,P)): PredicatedForComprehensionBuilder[V,R,P] =
      PredicatedForComprehensionBuilder[V,R,P](args._1, args._2, args._3)
  }

  implicit class IffImplicit[V <: VariableExpr[V], R <: RingExpr](pair: VariableRingPair[V,R]) {
    def iff[P <: RingExpr](p: P): (V,R,P) = (pair.k,pair.r,p)
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