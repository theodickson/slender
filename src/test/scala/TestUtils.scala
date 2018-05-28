package slender
import scala.reflect.runtime.universe._

trait TestUtils {
  def printType[T](t: T)(implicit ev: TypeTag[T]): Unit = println(ev.tpe)
//  def printEval[T, E <: Expr](e: E)(implicit eval: Eval[E,T]): Unit = println(e.eval)
}
