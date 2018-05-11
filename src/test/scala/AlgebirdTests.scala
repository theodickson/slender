//package slender
//
//import org.scalatest.FunSuite
//
//import com.twitter.algebird._
//import com.twitter.algebird.Operators._
//import com.twitter.algebird.Ring._
////import slender.algebra.implicits._
//
//import scala.tools.reflect.ToolBox
//import scala.reflect.runtime.currentMirror
//import scala.reflect.runtime.universe._
//
//import scala.util.Random
//
//class AlgebirdTests extends FunSuite {
//
//  val tb = currentMirror.mkToolBox()
//
//  def eval(code: Tree) = tb.eval(
//    q"""
//        object App {
//
//          import com.twitter.algebird._
//          import com.twitter.algebird.Operators._
//          import com.twitter.algebird.Ring._
//
//
//          import scala.util.Random
//
//          def result = Random.nextInt
//
//        }
//
//        App.result
//     """
//  )
//
//
//  test("Ring +") {
////    assert(toPlus(Map(1 -> 2)) + Map(1 -> 2) == Map(1 -> 4))
////    assert(toPlus((1,2)) + (3,4) == (4,6))
//    assert((new PlusOp[Int](1)(intRing)).+(2) == 3)
////    assert(Set(1,2,3) + Set(4,5,6) == Set(1,2,3,4,5,6))
//    assert(Map(1 -> 2) + Map(1 -> 2) == Map(1 -> 4))
//    println(eval(
//      q"""(new PlusOp[Int](1)(intRing)).+(2)"""
//    ))
//  }
//}
