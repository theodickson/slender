package slender

import org.scalatest.FunSuite
import shapeless._
import scala.reflect.runtime.universe._

class GenericTests extends FunSuite {

  import implicits._
  import TestUtils._

  test("") {
    val expr = Tuple2VariableExpr(Tuple2VariableExpr(X,Y),Z)
    def printGen[E <: Expr, R : TypeTag](e: E)(implicit gen: Generic.Aux[E,R]): Unit = printType(gen.to(e))
    printGen(expr)
  }
}
