package slender

trait Eval[-E <: Expr,+T] extends ((E,BoundVars) => T) with Serializable

trait CollectionEval[-E <: Expr,C[_,_],K,R] extends ((E,BoundVars) => C[K,R]) with Serializable

case class Label[R <: RingExpr,T](expr: R, vars: BoundVars, eval: Eval[R,T]) extends Serializable {
  def get: T = eval(expr,vars)
  def varString = vars.toList.map { case (variable,value) => s"$variable=$value" } mkString (",")
  override def toString = s"Label($varString)"
}

