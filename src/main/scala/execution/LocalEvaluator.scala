//package slender.execution
//
//import slender._
//
//object LocalEvaluator {
//  def apply(expr: Expr[_]): Any = expr match {
//    case p : PhysicalCollection[_,_] => { val x = eval(p); x
//  }
//
//  def eval[K,V](p: PhysicalCollection[K,V]): Map[K,V] = p.phys
//
//
//
//}
