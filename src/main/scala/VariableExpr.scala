package slender

trait VariableExpr extends KeyExpr {
  type Type
  def bind(t: Type): BoundVars
//  def <--[R <: RingExpr](r: R): (Self, R) = (this.asInstanceOf[Self],r)
//  def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[Self,R] = InfiniteMappingExpr(this.asInstanceOf[Self],r)
}

trait Variable[X <: UntypedVariable[X]] extends VariableExpr with NullaryKeyExpr //{ type O = T }

case class TypedVariable[X <: UntypedVariable[X],T](name: X) extends Variable[X] {
  type Self = TypedVariable[X,T]
  type Type = T
  override def toString = s""""$name""""
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

trait UntypedVariableExpr[T <: UntypedVariableExpr[T]] extends VariableExpr {
  type Self <: UntypedVariableExpr[T]
  type Type = Untyped
  def bind(t: Untyped) = ???
    def <--[R <: RingExpr](r: R): (T, R) = (this.asInstanceOf[T],r)
    def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[T,R] = InfiniteMappingExpr(this.asInstanceOf[T],r)
}

trait UntypedVariable[T <: UntypedVariable[T]] extends Variable[T] with UntypedVariableExpr[T] with NullaryKeyExpr {
  def tag[KT]: TypedVariable[T,KT] = TypedVariable[T,KT](this.asInstanceOf[T])
  override def toString = s""""$this:?""""
}

//sealed trait Tuple2VariableExpr[V1 <: VariableExpr,V2 <: VariableExpr] extends VariableExpr
//  with BinaryExpr with ProductExpr {
//  def c1: V1
//  def c2: V2
//}

case class Tuple2VariableExpr[V1 <: VariableExpr,V2 <: VariableExpr](c1: V1, c2: V2)
  extends VariableExpr with BinaryExpr with ProductExpr {
  type Self = Tuple2VariableExpr[V1,V2]
  type Type = (c1.Type,c2.Type)
  def bind(t: (c1.Type,c2.Type)) = c1.bind(t._1) ++ c2.bind(t._2)
}

//case class Tuple3VariableExpr[V1 <: VariableExpr[_],V2 <: VariableExpr[_],V3 <: VariableExpr[_],T1,T2,T3]
//(c1: V1, c2: V2, c3: V3)
//(implicit ev1: V1 <:< VariableExpr[T1], ev2: V2 <:< VariableExpr[T2], ev3: V3 <:< VariableExpr[T3])
//  extends VariableExpr[(T1,T2,T3)]
//    with TernaryExpr with ProductExpr {
//  type Self = Tuple3VariableExpr[V1,V2,V3,T1,T2,T3]
//  def bind(t: (T1,T2,T3)) = c1.bind(t._1) ++ c2.bind(t._2) ++ c3.bind(t._3)
//}

//case class Tuple2UntypedVariableExpr[V1 <: UntypedVariableExpr[V1], V2 <: UntypedVariableExpr[V2]](c1: V1, c2: V2)
//  extends UntypedVariableExpr[Tuple2UntypedVariableExpr[V1,V2]] with Tuple2VariableExpr[V1,V2,Untyped,Untyped] {
//  type Self = Tuple2UntypedVariableExpr[V1,V2]
//}


trait X extends UntypedVariable[X] { override def toString = "x" }
trait Y extends UntypedVariable[Y] { override def toString = "y" }
trait Z extends UntypedVariable[Z] { override def toString = "z" }
trait W extends UntypedVariable[W] { override def toString = "w" }

trait VariableExprImplicits {

  type Untyped

  implicit val X = new X {}
  implicit val Y = new Y {}
  implicit val Z = new Z {}
  implicit val W = new W {}

}

//object test {
//  def main(args: Array[String]): Unit = {
//    val variable = X
//    val tagged = variable.tag[Int]
//    val bound = tagged.bind(1)
//    println(bound)
//
//    val bagOfInts = PhysicalCollection(Map(1 -> 1))
//    val expr = SumExpr(MultiplyExpr(bagOfInts,InfiniteMappingExpr(TypedVariable[X,Int](X), IntExpr(1))))
//    println(expr.eval)
//  }
//}