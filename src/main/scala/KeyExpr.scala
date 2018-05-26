package slender

import scala.reflect.{ClassTag,classTag}

trait KeyExpr extends Expr { //self : Self =>

  type Self <: KeyExpr

  def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(this, k1)
  def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(this, k1))

  def >[K1 <: KeyExpr](k1: K1) = IntPredicate(this, k1, _ > _, ">")
  def <[K1 <: KeyExpr](k1: K1) = IntPredicate(this, k1, _ < _, "<")

  def -->[R <: RingExpr](r: R): (Self,R) = (this.asInstanceOf[Self],r)
}

trait NullaryKeyExpr extends KeyExpr with NullaryExpr
trait UnaryKeyExpr extends KeyExpr with UnaryExpr
trait BinaryKeyExpr extends KeyExpr with BinaryExpr
trait TernaryKeyExpr extends KeyExpr with TernaryExpr


case class PrimitiveKeyExpr[T](value: T) extends KeyExpr with PrimitiveExpr[T] {
  type Self = PrimitiveKeyExpr[T]
}
object IntKeyExpr {
  def apply(i: Int) = PrimitiveKeyExpr(i)
}
object StringKeyExpr {
  def apply(s: String) = PrimitiveKeyExpr(s)
}


case class Tuple2KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends BinaryKeyExpr with ProductExpr {
  type Self = Tuple2KeyExpr[K1,K2]
}

case class Tuple3KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryKeyExpr with ProductExpr {
  type Self = Tuple3KeyExpr[K1,K2,K3]
}


case class Project1KeyExpr[K <: KeyExpr with C1Expr](c1: K) extends UnaryKeyExpr with Project1Expr {
  type Self = Project1KeyExpr[K]
}

case class Project2KeyExpr[K <: KeyExpr with C2Expr](c1: K) extends UnaryKeyExpr with Project2Expr {
  type Self = Project2KeyExpr[K]
}

case class Project3KeyExpr[K <: KeyExpr with C3Expr](c1: K) extends UnaryKeyExpr with Project3Expr {
  type Self = Project3KeyExpr[K]
}


trait VariableExpr[T] extends KeyExpr {
  def bind(t: T): BoundVars
}

trait Variable[X <: UntypedVariable[X],T] extends VariableExpr[T] with NullaryKeyExpr

case class TypedVariable[X <: UntypedVariable[X],T](name: X) extends Variable[X,T] {
  type Self = TypedVariable[X,T]
  override def toString = s""""$name:}""""
  def bind(t: T) = Map(this -> t)
  //  override def explain: String = s""""$name": $exprType"""
  //  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = vars.get(name) match {
  //    case None | Some(`exprType`) => this
  //    case Some(otherType) => if (overwrite) TypedVariable(name, otherType) else
  //      throw VariableResolutionConflictException(
  //        s"Tried to resolve var $name with type $otherType, already had type $exprType, overwriting false."
  //      )
  //  }
  //  override def variables = Set(this)
  //  override def freeVariables = Set(this)
}

trait UntypedVariableExpr[T <: UntypedVariableExpr[T]] extends VariableExpr[T] {
  type Self = T
  def tag[KT]: 
}

trait UntypedVariable[T <: UntypedVariable[T]] extends Variable[T,Nothing] with NullaryKeyExpr {
  type Self = T// UntypedVariable
  def bind(t: Nothing) = ???
  def tag[KT]: TypedVariable[T,KT] = TypedVariable[T,KT](this.asInstanceOf[T])
  def <--[R <: RingExpr](r: R): (T, R) = (this.asInstanceOf[T],r)
  def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[T,R] = InfiniteMappingExpr(this.asInstanceOf[T],r)
}


case class Tuple2VariableExpr[V1 <: VariableExpr[_],V2 <: VariableExpr[_],T1,T2](c1: V1, c2: V2)
                                                                                (implicit ev1: V1 <:< VariableExpr[T1],
                                                                                          ev2: V2 <:< VariableExpr[T2])
  extends VariableExpr[(T1,T2)]
  with BinaryExpr with ProductExpr {
  type Self = Tuple2VariableExpr[V1,V2,T1,T2]
  def bind(t: (T1,T2)) = c1.bind(t._1) ++ c2.bind(t._2)
}

case class Tuple3VariableExpr[V1 <: VariableExpr[_],V2 <: VariableExpr[_],V3 <: VariableExpr[_],T1,T2,T3]
  (c1: V1, c2: V2, c3: V3)
  (implicit ev1: V1 <:< VariableExpr[T1], ev2: V2 <:< VariableExpr[T2], ev3: V3 <:< VariableExpr[T3])
  extends VariableExpr[(T1,T2,T3)]
    with TernaryExpr with ProductExpr {
  type Self = Tuple3VariableExpr[V1,V2,V3,T1,T2,T3]
  def bind(t: (T1,T2,T3)) = c1.bind(t._1) ++ c2.bind(t._2) ++ c3.bind(t._3)
}


trait ToK extends UnaryKeyExpr

case class BoxedRingExpr[R <: Expr](c1: R) extends ToK {
  type Self = BoxedRingExpr[R]
  override def toString = s"[$c1]"
  //  def _eval(vars: BoundVars) = c1._eval(vars)
  //  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
  //    BoxedRingExpr(c1.replaceTypes(vars, overwrite))
  //  def renest = ???
}

case class LabelExpr[R <: RingExpr](c1: R) extends UnaryKeyExpr {

  type Self = LabelExpr[R]

  override def toString = s"Label($id)"

  //  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")
  //
  //  def definition: String = {
  //    val s = new StringBuilder(s"$this: ${c1.exprType}")
  //    s ++= s"\n\n\t$c1"
  //    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
  //    s.mkString
  //  }
  //  override def labels = c1.labels :+ this
  //
  //  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
  //    LabelExpr(c1.replaceTypes(vars, overwrite))

  //  def renest = BoxedRingExpr(FromLabel(LabelExpr(c1.renest))) //todo

}