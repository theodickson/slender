package slender

import shapeless.{::, HList, HNil}

trait Binder[V, T] extends ((V,T) => BoundVars) with Serializable

object Binder {

  def instance[V,T](f: (V,T) => BoundVars): Binder[V,T] = new Binder[V,T] {
    def apply(v1: V, v2: T): BoundVars = f(v1,v2)
  }

  implicit def UnusedVariableBinder[T]: Binder[UnusedVariable,T] =
    instance { (_,_) => Map.empty[String,Any] }

  implicit def TypedVarBinder[T]: Binder[TypedVariable[T],T] =
    instance { (v1,v2) => Map(v1.name -> v2) }

  implicit def HListVarBinder[VH:Expr, VT <: HList, H, T <: HList]
  (implicit bindH: Binder[VH,H], bindT: Binder[VT,T]): Binder[VH :: VT, H::T] =
    instance { case ((vh::vt),(h::t)) => bindH(vh,h) ++ bindT(vt,t) }

  implicit def HNilVarBinder[T]: Binder[HNil,T] = instance { (_,_) => Map.empty[String,Any] }
}

//  implicit def ProductVarBinder2[V1 <: Expr, V2 <: Expr, T1, T2]
//  (implicit bind1: VarBinder[V1,T1], bind2: VarBinder[V2,T2]):
//  VarBinder[ProductExpr[V1 :: V2 :: HNil], (T1,T2)] =
//    new VarBinder[ProductExpr[V1 :: V2 :: HNil], (T1,T2)] {
//      def apply(v1: ProductExpr[V1 :: V2 :: HNil], v2: (T1,T2)): BoundVars = bind1(v1.exprs.head, v2._1) ++ bind2(v1.exprs.tail.head, v2._2)
//    }
//
//  implicit def ProductVarBinder3[V1 <: Expr, V2 <: Expr, V3 <: Expr, T1, T2, T3]
//  (implicit bind1: VarBinder[V1,T1], bind2: VarBinder[V2,T2], bind3: VarBinder[V3,T3]):
//  VarBinder[ProductExpr[V1 :: V2 :: V3 :: HNil], (T1,T2,T3)] =
//    new VarBinder[ProductExpr[V1 :: V2 :: V3 :: HNil], (T1,T2,T3)] {
//      def apply(v1: ProductExpr[V1 :: V2 :: V3 :: HNil], v2: (T1,T2,T3)): BoundVars =
//        bind1(v1.exprs.head, v2._1) ++ bind2(v1.exprs.tail.head, v2._2) ++ bind3(v1.exprs.tail.tail.head, v2._3)
//    }
