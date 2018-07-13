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
