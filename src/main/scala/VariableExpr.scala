package slender

trait VariableExpr[V <: VariableExpr[V]] extends KeyExpr[V] { self: V =>
  type Type
  def bind(t: Type): BoundVars
  def <--[R <: RingExpr[R]](r: R): (V,R) = (this,r)
  def ==>[R <: RingExpr[R]](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr[V,R](this,r)
}


case class TypedVariable[T](name: String) extends VariableExpr[TypedVariable[T]] with NullaryExpr {
  type Self = TypedVariable[T]
  type Type = T
  override def toString = name
  def bind(t: T) = Map(this.name -> t)
  override def isResolved = true
}


trait UntypedVariable[T <: VariableExpr[T]] extends VariableExpr[T] with NullaryExpr { self: T =>
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  type Type = Untyped
  def bind(t: Untyped) = ???
  override def toString = s""""$this:?""""
  override def isResolved = false
}

case class Tuple2VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2]](c1: V1, c2: V2)
  extends VariableExpr[Tuple2VariableExpr[V1,V2]] with BinaryExpr {
  type Self = Tuple2VariableExpr[V1,V2]
  type Type = (c1.Type,c2.Type)
  def bind(t: (c1.Type,c2.Type)) = c1.bind(t._1) ++ c2.bind(t._2)
}

case class Tuple3VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2],V3 <: VariableExpr[V3]](c1: V1, c2: V2, c3: V3)
  extends VariableExpr[Tuple3VariableExpr[V1,V2,V3]] with BinaryExpr {
  type Self = Tuple3VariableExpr[V1,V2,V3]
  type Type = (c1.Type,c2.Type,c3.Type)
  def bind(t: (c1.Type,c2.Type,c3.Type)) = c1.bind(t._1) ++ c2.bind(t._2) ++ c3.bind(t._3)
}


trait _X extends UntypedVariable[_X] { override def name = "x" }
trait _Y extends UntypedVariable[_Y] { override def name = "y" }
trait _Z extends UntypedVariable[_Z] { override def name = "z" }
trait _W extends UntypedVariable[_W] { override def name = "w" }

trait _X1 extends UntypedVariable[_X1] { override def name = "x1" }
trait _Y1 extends UntypedVariable[_Y1] { override def name = "y1" }
trait _Z1 extends UntypedVariable[_Z1] { override def name = "z1" }
trait _W1 extends UntypedVariable[_W1] { override def name = "w1" }

trait Variables {
  val X = new _X {}
  val Y = new _Y {}
  val Z = new _Z {}
  val W = new _W {}

  val X1 = new _X1 {}
  val Y1 = new _Y1 {}
  val Z1 = new _Z1 {}
  val W1 = new _W1 {}

}