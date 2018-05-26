package slender

trait RingExpr extends Expr { //self : Self =>

//  type Self <: RingExpr
//
//  def +[R1 <: RingExpr](expr1: R1) = AddExpr(this,expr1)
//
//  def *[R1 <: RingExpr](expr1: R1) = MultiplyExpr(this,expr1)
//
//  def dot[R1 <: RingExpr](expr1: R1) = DotExpr(this,expr1)
//
//  def sum = SumExpr(this)
//
//  def unary_- = NegateExpr(this)
//
//  def unary_! = NotExpr(this)

}

trait NullaryRingExpr extends RingExpr with NullaryExpr
trait UnaryRingExpr extends RingExpr with UnaryExpr
trait BinaryRingExpr extends RingExpr with BinaryExpr
trait TernaryRingExpr extends RingExpr with TernaryExpr

trait PrimitiveRingExpr[T] extends RingExpr with PrimitiveExpr[T]

case class IntExpr(value: Int) extends PrimitiveRingExpr[Int] {
  type Self = IntExpr
}

case class PhysicalCollection[C[_,_],K,R](value: C[K,R])
  extends PrimitiveRingExpr[C[K,R]] {
  type Self = PhysicalCollection[C,K,R]
}

case class Tuple2RingExpr[K1 <: RingExpr, K2 <: RingExpr](c1: K1, c2: K2) extends BinaryRingExpr with ProductExpr {
  type Self = Tuple2RingExpr[K1,K2]
}

case class Tuple3RingExpr[K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryRingExpr with ProductExpr {
  type Self = Tuple3RingExpr[K1,K2,K3]
}


case class Project1RingExpr[K <: RingExpr with C1Expr](c1: K) extends UnaryRingExpr with Project1Expr {
  type Self = Project1RingExpr[K]
}

case class Project2RingExpr[K <: RingExpr with C2Expr](c1: K) extends UnaryRingExpr with Project2Expr {
  type Self = Project2RingExpr[K]
}

case class Project3RingExpr[K <: RingExpr with C3Expr](c1: K) extends UnaryRingExpr with Project3Expr {
  type Self = Project3RingExpr[K]
}


case class InfiniteMappingExpr[K <: VariableExpr[_],KT,R <: RingExpr](key: K, value: R)(implicit ev: K <:< VariableExpr[KT])
  extends BinaryRingExpr {

  type Self = InfiniteMappingExpr[K,KT,R]
  def c1 = key; def c2 = value

  override def toString = s"{$key => $value}"

  //  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): RingExpr =
  //    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))
  //

  //  def renest = InfiniteMappingExpr(key.renest, value.renest)
}


//case class InfiniteMappingExpr1[K <: Variable[_,_],V <: UntypedVariable,KT,R <: RingExpr](key: K, value: R)(implicit ev: K <:< Variable[V,KT])
//  extends BinaryRingExpr {
//
//  type Self = InfiniteMappingExpr1[K,V,KT,R]
//  def c1 = key; def c2 = value
//
//  override def toString = s"{$key => $value}"
//
//  //  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): RingExpr =
//  //    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))
//  //
//
//  //  def renest = InfiniteMappingExpr(key.renest, value.renest)
//}


trait BinaryRingOpExpr extends BinaryRingExpr {
  def opString: String
  //  final def brackets = ("(",")")
  //  override def toString = s"${c1.closedString} $opString ${c2.closedString}"
}


case class MultiplyExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {

  type Self = MultiplyExpr[E1,E2]
  def opString = "*"
  //  override def freeVariables = c2 match {
  //    case InfiniteMappingExpr(k: VariableKeyExpr, _) => c1.freeVariables ++ c2.freeVariables -- k.freeVariables
  //    case _ => c1.freeVariables ++ c2.freeVariables
  //  }

  //  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = {
  //    val newC1 = c1.replaceTypes(vars, overwrite)
  //    val newC2 = newC1.exprType match {
  //      case FiniteMappingType(keyType,_,_) => c2 match {
  //        case InfiniteMappingExpr(v: VariableKeyExpr,_) => c2.replaceTypes(vars ++ v.matchTypes(keyType), overwrite)
  //        case _ => c2.replaceTypes(vars, overwrite)
  //      }
  //      case _ => c2.replaceTypes(vars, overwrite)
  //    }
  //    Multiply(newC1, newC2)
  //  }
  //
  //  def shred = {
  //    val newC1 = c1.shred
  //    val newC2 = newC1.exprType match {
  //      case FiniteMappingType(keyType,_,_) => c2 match {
  //        case InfiniteMappingExpr(v: VariableKeyExpr,_) => {
  //          val replaced = c2.replaceTypes(v.matchTypes(keyType), true)
  //          replaced.shred
  //        }
  //        case _ => c2.shred
  //      }
  //      case _ => c2.shred
  //    }
  //    Multiply(newC1, newC2)
  //  }
  //
  //  def renest = { //todo refactor
  //    val newC1 = c1.renest
  //    val newC2 = newC1.exprType match {
  //      case FiniteMappingType(keyType,_,_) => c2 match {
  //        case InfiniteMappingExpr(v: VariableKeyExpr,_) => {
  //          val replaced = c2.replaceTypes(v.matchTypes(keyType), true)
  //          replaced.renest
  //        }
  //        case _ => c2.renest
  //      }
  //      case _ => c2.renest
  //    }
  //    Multiply(newC1, newC2)
  //  }
}

case class AddExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {

  type Self = AddExpr[E1,E2]
  def opString = "+"
}

case class DotExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {

  type Self = DotExpr[E1,E2]
  def opString = "⊙"
}

case class SumExpr[E <: Expr](c1: E) extends UnaryRingExpr {
  type Self = SumExpr[E]
}

case class NotExpr[E <: Expr](c1: E) extends UnaryRingExpr {
  type Self = NotExpr[E]
}

case class NegateExpr[E <: Expr](c1: E) extends UnaryRingExpr {
  type Self = NegateExpr[E]
}


trait Predicate extends BinaryRingExpr {
  def opString: String
//  override def brackets = ("(",")")
  override def toString = s"$c1 $opString $c2"
}

case class EqualsPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends Predicate {
  type Self = EqualsPredicate[K1,K2]
  def opString = "="
}

case class IntPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2, p: (Int,Int) => Boolean, opString: String) extends Predicate {
  type Self = IntPredicate[K1, K2]
}

object Predicate {
  def apply[K1 <: KeyExpr, K2 <: KeyExpr](k1: K1, k2: K2) = EqualsPredicate(k1,k2)
}

case class SngExpr[K <: KeyExpr,R <: RingExpr](key: K, value: R)
  extends BinaryRingExpr {
  type Self = SngExpr[K,R]
  def c1 = key; def c2 = value
}
//
//
////sealed trait FromK extends RingExpr with UnaryExpr[E[KeyExpr]
////
////object FromK {
////  def apply(k: KeyExpr): FromK = k.exprType match {
////    case BoxedRingType(_) => FromBoxedRing(k)
////    case LabelType(_) => FromLabel(k)
////    case t => throw InvalidFromKException(s"Cannot create ring expr from key expr with type $t")
////  }
////}
////
//case class FromBoxedRing[R,E <: Expr[R,E]](c1: BoxedRingExpr[R,E]) extends Expr[R,FromBoxedRing[R,E]] with UnaryExpr[R,BoxedRingExpr[R,E]] {
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    FromBoxedRing(c1.replaceTypes(vars, overwrite))
////
//  def _eval(vars: BoundVars): R = c1._eval(vars)
//}
//
//
//case class FromLabel[O,E <: Expr[O,E]](c1: LabelExpr[O,E]) extends Expr[O,FromLabel[O,E]] with UnaryExpr[Label[O,E],LabelExpr[O,E]] {
////
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = FromLabel(c1.replaceTypes(vars, overwrite))
//
//  def _eval(vars: BoundVars): O = c1._eval(vars).eval //todo - do I actually need to use the free variables stored in the label?
//  def shred = ???
////  def renest = FromK(c1.renest)
//}