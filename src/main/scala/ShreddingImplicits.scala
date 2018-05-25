package slender

trait ShreddingImplicits {

  implicit def VariableShredder[K]: NonShredder[Variable[K]] = new NonShredder[Variable[K]] { }

  implicit def PrimitiveExprShredder[E <: PrimitiveExpr[_]]: NonShredder[E] = new NonShredder[E] {}

//  implicit def InfiniteMappingShredder[K,R <: RingExpr,RS <: RingExpr](implicit shred: Shredder[R,RS]):
//  Shredder[InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,RS]] =
//    new Shredder[InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,RS]] {
//      def apply(v1: InfiniteMappingExpr[K,R]): InfiniteMappingExpr[K,RS] = InfiniteMappingExpr(v1.key,shred(v1.value))
//    }

  implicit def MultiplyShredder[R1 <: RingExpr, R2 <: RingExpr, R1S <: RingExpr, R2S <: RingExpr]
  (implicit shred1: Shredder[R1,R1S], shred2: Shredder[R2,R2S]): Shredder[MultiplyExpr[R1,R2],MultiplyExpr[R1S,R2S]] =
    new Shredder[MultiplyExpr[R1,R2],MultiplyExpr[R1S,R2S]] {
      def apply(v1: MultiplyExpr[R1,R2]): MultiplyExpr[R1S,R2S] = MultiplyExpr(shred1(v1.c1),shred2(v1.c2))
    }

  implicit def DotShredder[R1 <: RingExpr, R2 <: RingExpr, R1S <: RingExpr, R2S <: RingExpr]
  (implicit shred1: Shredder[R1,R1S], shred2: Shredder[R2,R2S]): Shredder[DotExpr[R1,R2],DotExpr[R1S,R2S]] =
    new Shredder[DotExpr[R1,R2],DotExpr[R1S,R2S]] {
      def apply(v1: DotExpr[R1,R2]): DotExpr[R1S,R2S] = DotExpr(shred1(v1.c1),shred2(v1.c2))
    }

  implicit def AddShredder[R1 <: RingExpr, R2 <: RingExpr, R1S <: RingExpr, R2S <: RingExpr]
  (implicit shred1: Shredder[R1,R1S], shred2: Shredder[R2,R2S]): Shredder[AddExpr[R1,R2],AddExpr[R1S,R2S]] =
    new Shredder[AddExpr[R1,R2],AddExpr[R1S,R2S]] {
      def apply(v1: AddExpr[R1,R2]): AddExpr[R1S,R2S] = AddExpr(shred1(v1.c1),shred2(v1.c2))
    }

  implicit def SumShredder[R1 <: RingExpr, R1S <: RingExpr](implicit shred1: Shredder[R1,R1S]):
  Shredder[SumExpr[R1],SumExpr[R1S]] = new Shredder[SumExpr[R1],SumExpr[R1S]] {
    def apply(v1: SumExpr[R1]): SumExpr[R1S] = SumExpr(shred1(v1.c1))
  }

  implicit def BoxedRingShredder[R <: RingExpr,RS <: RingExpr](implicit shred: Shredder[R,RS]):
  Shredder[BoxedRingExpr[R],LabelExpr[RS]] = new Shredder[BoxedRingExpr[R],LabelExpr[RS]] {
    def apply(v1: BoxedRingExpr[R]): LabelExpr[RS] = LabelExpr(shred(v1.c1))
  }

  implicit def SngShredder[K <: KeyExpr,R <: RingExpr,KS <: KeyExpr,RS <: RingExpr]
  (implicit shredK: Shredder[K,KS], shredR: Shredder[R,RS]): Shredder[SngExpr[K,R],SngExpr[KS,RS]] =
    new Shredder[SngExpr[K,R],SngExpr[KS,RS]] {
      def apply(v1: SngExpr[K,R]): SngExpr[KS,RS] = SngExpr(shredK(v1.key),shredR(v1.value))
    }

}