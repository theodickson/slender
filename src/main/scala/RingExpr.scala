package slender

trait RingExpr[E <: Expr[E]] extends Expr[E] { self : E =>

  def +[R1 <: Expr[R1]](expr1: R1): AddExpr[E,R1] = AddExpr[E,R1](self,expr1)

  def *[R1 <: Expr[R1]](expr1: R1): MultiplyExpr[E,R1] = MultiplyExpr[E,R1](self,expr1)

  def dot[R1 <: Expr[R1]](expr1: R1): DotExpr[E,R1] = DotExpr[E,R1](self,expr1)

  def sum: SumExpr[E] = SumExpr[E](self)
//
//  def unary_- = NegateExpr(this)
//
//  def unary_! = NotExpr(this)

//  def && = self.* _
//
//  def || = self.+ _

}

//trait NullaryRingExpr extends RingExpr with NullaryExpr
//trait UnaryRingExpr extends RingExpr with UnaryExpr
//trait BinaryRingExpr extends RingExpr with BinaryExpr
//trait TernaryRingExpr extends RingExpr with TernaryExpr

//trait PrimitiveRingExpr[T] extends RingExpr with PrimitiveExpr[T]

case class IntExpr(value: Int) extends RingExpr[IntExpr] with PrimitiveExpr[Int,IntExpr] {
  type Self = IntExpr
}

case class PhysicalCollection[C[_,_],K,R](value: C[K,R])
  extends RingExpr[PhysicalCollection[C,K,R]] with PrimitiveExpr[C[K,R],PhysicalCollection[C,K,R]]

//case class Tuple2RingExpr[K1 <: RingExpr, K2 <: RingExpr](c1: K1, c2: K2) extends BinaryRingExpr with ProductExpr {
//  type Self = Tuple2RingExpr[K1,K2]
//}
//
//case class Tuple3RingExpr[K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr](c1: K1, c2: K2, c3: K3)
//  extends TernaryRingExpr with ProductExpr {
//  type Self = Tuple3RingExpr[K1,K2,K3]
//}
//
//
//case class Project1RingExpr[K <: RingExpr with C1Expr](c1: K) extends UnaryRingExpr with Project1Expr {
//  type Self = Project1RingExpr[K]
//}
//
//case class Project2RingExpr[K <: RingExpr with C2Expr](c1: K) extends UnaryRingExpr with Project2Expr {
//  type Self = Project2RingExpr[K]
//}
//
//case class Project3RingExpr[K <: RingExpr with C3Expr](c1: K) extends UnaryRingExpr with Project3Expr {
//  type Self = Project3RingExpr[K]
//}

case class InfiniteMappingExpr[K <: VariableExpr[K],R <: Expr[R]](key: K, value: R)
  extends Expr[InfiniteMappingExpr[K,R]] with BinaryExpr {

  def c1 = key; def c2 = value

  override def toString = s"{$key => $value}"

}


case class MultiplyExpr[E1 <: Expr[E1],E2 <: Expr[E2]](c1: E1, c2: E2)
  extends Expr[MultiplyExpr[E1,E2]] with BinaryExpr {

  def opString = "*"
}

case class AddExpr[E1 <: Expr[E1],E2 <: Expr[E2]](c1: E1, c2: E2)
  extends Expr[AddExpr[E1,E2]] with BinaryExpr {

  def opString = "+"
}

case class DotExpr[E1 <: Expr[E1],E2 <: Expr[E2]](c1: E1, c2: E2)
  extends Expr[DotExpr[E1,E2]] with BinaryExpr {

  def opString = "⊙"
}

case class SumExpr[E <: Expr[E]](c1: E) extends Expr[SumExpr[E]] with UnaryExpr {
  type Self = SumExpr[E]
}

//case class NotExpr[E <: Expr[E]](c1: E) extends Expr[UnaryExpr {
//  type Self = NotExpr[E]
//}
//
//case class NegateExpr[E <: Expr](c1: E) extends UnaryExpr {
//  type Self = NegateExpr[E]
//}
//

//trait Predicate extends BinaryRingExpr {
//  def opString: String
////  override def brackets = ("(",")")
//  override def toString = s"$c1 $opString $c2"
//}
//
//case class EqualsPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends Predicate {
//  type Self = EqualsPredicate[K1,K2]
//  def opString = "="
//}
//
//case class IntPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2, p: (Int,Int) => Boolean, opString: String)
//  extends Predicate {
//  type Self = IntPredicate[K1, K2]
//}
//
//object Predicate {
//  def apply[K1 <: KeyExpr, K2 <: KeyExpr](k1: K1, k2: K2) = EqualsPredicate(k1,k2)
//}

case class SngExpr[K <: Expr[K],R <: Expr[R]](key: K, value: R)
  extends Expr[SngExpr[K,R]] with BinaryExpr {
  type Self = SngExpr[K,R]
  def c1 = key; def c2 = value
}

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

//case class ToRingExpr[E <: Expr](c1: E) extends UnaryRingExpr {
//  type Self = ToRingExpr[E]
//}
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