/**The interface which binds variables in a namespace*/

package slender

import shapeless.{::, HList, HNil}

/**It's a function from a variable expression and a type to a namespace containing the individual variables and
  * their associated types. E.g. with V: (TypedVariable[Int]("x"),TypedVariable[String]("y")) and T: (1,"a"),
  * it would produce:
  *   Map[String,Any]("x" -> 1, "y" -> "a")
  */
trait Binder[V, T] extends ((V,T) => Namespace) with Serializable

object Binder {

  def instance[V,T](f: (V,T) => Namespace): Binder[V,T] = new Binder[V,T] {
    def apply(v1: V, v2: T): Namespace = f(v1,v2)
  }

  /**For an unused variable (represented by '__' in a query), don't bind as it will never of course be used. */
  implicit def UnusedVariableBinder[T]: Binder[UnusedVariable,T] =
    instance { (_,_) => Map.empty[String,Any] }

  /**For a single typed variable, bind it to the associated type*/
  implicit def TypedVarBinder[T]: Binder[TypedVariable[T],T] =
    instance { (v1,v2) => Map(v1.name -> v2) }

  /**For a product variable expression and associated product value, recursively bind the tail expression to the tail
    * value and then bind the head expression to the head value*/
  implicit def HListVarBinder[VH:Expr, VT <: HList, H, T <: HList]
  (implicit bindH: Binder[VH,H], bindT: Binder[VT,T]): Binder[VH :: VT, H::T] =
    instance { case ((vh::vt),(h::t)) => bindH(vh,h) ++ bindT(vt,t) }

  /**The base case of a product type - return an empty namespace*/
  implicit def HNilVarBinder[T]: Binder[HNil,T] = instance { (_,_) => Map.empty[String,Any] }
}
