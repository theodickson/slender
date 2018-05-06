package slender.execution

import slender._

import scala.reflect.runtime.universe._

object Ring {

  def plus(r1: Any, r2: Any): Any = (r1,r2) match {
    case (r11: Int, r21: Int) => r11 + r21
  }

  def zero[T : TypeTag]: Any = {
    typeOf[T] match {
      case t if t =:= typeOf[Int] => 0
    }
  }

  def sum[T: TypeTag](list: List[T]): Any = list.fold(zero[T])(plus)

}

trait ListCollectionLike[T, K, V] {
  def list: List[T]
//  def getKey = T => K
//  def getValue = T => V
  //def sum: V = list.foldRight[V](zero)()
}

case class ListCollection[T, K, V](list: List[T]) extends ListCollectionLike[T, K, V]

case class ListBag[T](list: List[(T,Int)]) extends ListCollectionLike[(T,Int), T, Int] {
//  def getKey(p: (T,Int)) = p._1
//  def getValue(p: (T,Int)) = p._2
}

case class SimpleListCollection[T](list: List[T]) extends ListCollectionLike[T, T, Int] {
//  val getKey: T => T = identity[T]
//  val getValue: T => Int = _ => 1
}

case class ListExecutionContext(collections: (String,ListCollectionLike[_,_,_])*) extends ExecutionContext {
  def get(ref: String) = collections.filter(_._1 == ref).head._2
}

//object ListExecutor extends Executor[ListExecutionContext] {
//
//  def execute(expr: RingExpr, ctx: ListExecutionContext): Any = expr match {
//    case IntExpr(i) => i
//    case PhysicalCollection(kt,vt,ref) => ctx.get(ref).list.asInstanceOf[List[(kt.Type,vt.Type)]]
//    case PhysicalBag(kt,ref) => ctx.get(ref).list.asInstanceOf[List[(kt.Type,Int)]]
//    case Sum(c) => {
//      val inner = execute(c, ctx).asInstanceOf[List[innerRingType.Type]]
//      Ring.sum(inner)
//    }
//  }
//}



//object ListExecutionTypes extends ExecutionTypeContext {
//  def get(ringType: RingType) = ringType match {
//    case IntType => Int
//    case MappingType(kT,rT) => List[]
//  }
//  def get(keyType: KeyType): Nothing  = keyType match {
//    case UnitType => Unit
//    case IntKeyType => Int
//    case StringKeyType => String
//    case KeyPairType(k1,k2) => (get(k1), get(k2))
//  }
//}
