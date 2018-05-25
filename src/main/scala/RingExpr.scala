package slender

trait RingExpr extends Expr

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


case class InfiniteMappingExpr[K,R <: RingExpr](key: VariableExpr[K], value: R)
  extends BinaryRingExpr {

  type Self = InfiniteMappingExpr[K,R]
  def c1 = key; def c2 = value

  override def toString = s"{$key => $value}"

  //  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): RingExpr =
  //    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))
  //

  //  def renest = InfiniteMappingExpr(key.renest, value.renest)
}


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

case class DotExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {

  type Self = DotExpr[E1,E2]
  def opString = "⊙"
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

case class SumExpr[E <: Expr](c1: E) extends UnaryRingExpr {
  type Self = SumExpr[E]
  //  def _eval(vars: BoundVars): R = coll.sum(c1._eval(vars))
  //  override val exprType: RingType = c1.exprType.sum
  //  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Sum(c1.replaceTypes(vars, overwrite))
  //  def shred = Sum(c1.shred)
  //  def renest = Sum(c1.renest)
}
////
////
////case class Not(c1: RingExpr) extends UnaryRingExpr {
////  val exprType = c1.exprType
////  override def toString = s"¬${c1.closedString}"
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Not(c1.replaceTypes(vars, overwrite))
////  def shred = Not(c1.shred)
////  def renest = Not(c1.renest)
////}
////
////
////case class Negate(c1: RingExpr) extends UnaryRingExpr {
////  val exprType = c1.exprType
////  override def toString = s"-${c1.closedString}"
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    Negate(c1.replaceTypes(vars, overwrite))
////  def shred = Negate(c1.shred)
////  def renest = Negate(c1.renest)
////}
////
////trait Predicate extends RingExpr with BinaryExpr[KeyExpr,KeyExpr] {
////  def opString: String
////  override def brackets = ("(",")")
////  override def toString = s"$c1 $opString $c2"
////}
////
////case class EqualsPredicate(c1: KeyExpr, c2: KeyExpr) extends Predicate {
////  val exprType = c1.exprType compareEq c2.exprType
////  def opString = "="
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    EqualsPredicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))
////  def shred = EqualsPredicate(c1.shred, c2.shred)
////  def renest = EqualsPredicate(c1.renest, c2.renest)
////}
////
////case class IntPredicate(c1: KeyExpr, c2: KeyExpr, p: (Int,Int) => Boolean, opString: String)
////                       (implicit val ev1: TypeTag[(Int,Int) => Boolean])
////  extends Predicate {
////  val exprType = c1.exprType compareOrd c2.exprType
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    IntPredicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite), p, opString)
////  def shred = IntPredicate(c1.shred, c2.shred, p, opString)
////  def renest = IntPredicate(c1.renest, c2.renest, p, opString)
////  def literal = reify(p)
////}
////
//////
case class SngExpr[K <: KeyExpr,R <: RingExpr](key: K, value: R)
  extends BinaryRingExpr {

  def c1 = key; def c2 = value

  //  def _eval(vars: BoundVars): Map[K,R] = Map(key._eval(vars) -> value._eval(vars))

  //  override def toString = value match {
  //    case IntExpr(1) => s"sng($key)"
  //    case _ => s"sng($key, $value)"
  //  }

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