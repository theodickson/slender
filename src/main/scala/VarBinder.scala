package slender

import shapeless.{::, HNil}

trait VarBinder[V <: Expr, T] extends ((V,T) => BoundVars) with Serializable

object VarBinder {

  implicit def UnusedVariableBinder[T]: VarBinder[UnusedVariable,T] = new VarBinder[UnusedVariable,T] {
    def apply(v1: UnusedVariable, v2: T): BoundVars = Map.empty[String,Any]
  }

  implicit def TypedVarBinder[T]: VarBinder[TypedVariable[T],T] = new VarBinder[TypedVariable[T],T] {
    def apply(v1: TypedVariable[T], v2: T): BoundVars = Map(v1.name -> v2)
  }

  implicit def ProductVarBinder2[V1 <: Expr, V2 <: Expr, T1, T2]
  (implicit bind1: VarBinder[V1,T1], bind2: VarBinder[V2,T2]):
  VarBinder[ProductExpr[V1 :: V2 :: HNil], (T1,T2)] =
    new VarBinder[ProductExpr[V1 :: V2 :: HNil], (T1,T2)] {
      def apply(v1: ProductExpr[V1 :: V2 :: HNil], v2: (T1,T2)): BoundVars = bind1(v1.exprs.head, v2._1) ++ bind2(v1.exprs.tail.head, v2._2)
    }

  implicit def ProductVarBinder3[V1 <: Expr, V2 <: Expr, V3 <: Expr, T1, T2, T3]
  (implicit bind1: VarBinder[V1,T1], bind2: VarBinder[V2,T2], bind3: VarBinder[V3,T3]):
  VarBinder[ProductExpr[V1 :: V2 :: V3 :: HNil], (T1,T2,T3)] =
    new VarBinder[ProductExpr[V1 :: V2 :: V3 :: HNil], (T1,T2,T3)] {
      def apply(v1: ProductExpr[V1 :: V2 :: V3 :: HNil], v2: (T1,T2,T3)): BoundVars =
        bind1(v1.exprs.head, v2._1) ++ bind2(v1.exprs.tail.head, v2._2) ++ bind3(v1.exprs.tail.tail.head, v2._3)
    }
}
