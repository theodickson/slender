package slender

trait VariableExpr[V <: VariableExpr[V]] extends KeyExpr { self: V =>
  type Type
  def bind(t: Type): BoundVars
  def <--[R <: RingExpr](r: R): (V,R) = (this,r)
  def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(this,r)
//  def <--[R <: RingExpr](r: R): (Self, R) = (this.asInstanceOf[Self],r)
//  def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[Self,R] = InfiniteMappingExpr(this.asInstanceOf[Self],r)
}

//trait Variable[X <: UntypedVariable[X]] extends VariableExpr[Variable[X]] with NullaryKeyExpr //{ type O = T }

case class TypedVariable[T](name: String) extends VariableExpr[TypedVariable[T]] with NullaryKeyExpr {
  type Self = TypedVariable[T]
  type Type = T
  override def toString = name
  def bind(t: T) = Map(this.name -> t)
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

//trait UntypedVariableExpr[T <: UntypedVariableExpr[T]] extends VariableExpr[UntypedVariableExpr[T]] {
//  type Self <: UntypedVariableExpr[T]
//  type Type = Untyped
//  def bind(t: Untyped) = ???
////    def <--[R <: RingExpr](r: R): (T, R) = (this.asInstanceOf[T],r)
////    def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[T,R] = InfiniteMappingExpr(this.asInstanceOf[T],r)
//}

trait UntypedVariable[T <: VariableExpr[T]] extends VariableExpr[T] with NullaryKeyExpr { self: T =>
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  type Type = Untyped
  def bind(t: Untyped) = ???
  override def toString = s""""$this:?""""
}

case class Tuple2VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2]](c1: V1, c2: V2)
  extends VariableExpr[Tuple2VariableExpr[V1,V2]] with BinaryExpr with ProductExpr {
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

//trait NonBind { type Type; def bind(t: Type): BoundVars = ??? }
//case class _X() extends VariableExpr[_X] with NullaryKeyExpr with NonBind { override def toString = "x" }
trait X extends VariableExpr[X] with UntypedVariable[X] { override def name = "x" }
trait Y extends VariableExpr[Y] with UntypedVariable[Y] { override def name = "y" }
trait Z extends VariableExpr[Z] with UntypedVariable[Z] { override def name = "z" }
trait W extends VariableExpr[W] with UntypedVariable[W] { override def name = "w" }

trait VariableExprImplicits {

  type Untyped

//  implicit val X = _X()
    implicit val X = new X {}
    implicit val Y = new Y {}
    implicit val Z = new Z {}
    implicit val W = new W {}

}