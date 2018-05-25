package slender.execution

import slender._
import slender.algebra.Dot
import slender.types.BoundVars

trait Evaluator[E <: Expr[E],T] extends ((E,BoundVars) => T)

//todo with external evaluation, there is scope to put variables into the type system and then possibly
//remove the need to tag them
object implicits {

  implicit def VariableEvaluator[T]: Evaluator[Variable[T],T] = new Evaluator[Variable[T],T] {
    def apply(v1: Variable[T],v2: BoundVars): T = v2.get(v1).asInstanceOf[T]
  }

  implicit def PhysicalCollectionEvaluator[C[_,_],K,R]: Evaluator[PhysicalCollection[C,K,R],C[K,R]] = new Evaluator[PhysicalCollection[C,K,R],C[K,R]]{
    def apply(v1: PhysicalCollection[C,K,R],v2:BoundVars): C[K,R] = v1.value
  }

  implicit def SelfDotEvaluator[E <: Expr[E],T,O](implicit evaluator: Evaluator[E,T], dot: Dot[T,T,O]):
    Evaluator[SelfDotExpr[E],O] = new Evaluator[SelfDotExpr[E],O] {
      def apply(v1: SelfDotExpr[E], v2: BoundVars): O = dot(evaluator(v1.c1,v2),evaluator(v1.c2,v2))
  }
}
