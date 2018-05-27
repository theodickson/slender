package slender

trait VariableExpr[V <: VariableExpr[V]] extends KeyExpr { self: V =>
  type Type
  def bind(t: Type): BoundVars
  def <--[R <: RingExpr](r: R): (V,R) = (this,r)
  def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(this,r)
}


case class TypedVariable[T](name: String) extends VariableExpr[TypedVariable[T]] with NullaryKeyExpr {
  type Self = TypedVariable[T]
  type Type = T
  override def toString = name
  def bind(t: T) = Map(this.name -> t)
}


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


trait _X extends VariableExpr[_X] with UntypedVariable[_X] { override def name = "x" }
trait _Y extends VariableExpr[_Y] with UntypedVariable[_Y] { override def name = "y" }
trait _Z extends VariableExpr[_Z] with UntypedVariable[_Z] { override def name = "z" }
trait _W extends VariableExpr[_W] with UntypedVariable[_W] { override def name = "w" }

trait _X1 extends VariableExpr[_X1] with UntypedVariable[_X1] { override def name = "x" }
trait _Y1 extends VariableExpr[_Y1] with UntypedVariable[_Y1] { override def name = "y" }
trait _Z1 extends VariableExpr[_Z1] with UntypedVariable[_Z1] { override def name = "z" }
trait _W1 extends VariableExpr[_W1] with UntypedVariable[_W1] { override def name = "w" }

trait VariableExprImplicits {

  val X = new _X {}
  val Y = new _Y {}
  val Z = new _Z {}
  val W = new _W {}

  val X1 = new _X1 {}
  val Y1 = new _Y1 {}
  val Z1 = new _Z1 {}
  val W1 = new _W1 {}

}