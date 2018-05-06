package slender

sealed trait KeyExpr {
  def keyType: KeyType
  def freeVariables: Set[TypedFreeVariable]
  def shred: KeyExpr = this
}

sealed trait NullaryKeyExpr extends KeyExpr {
  def freeVariables = Set.empty
}

sealed trait Tuple2KeyExpr extends KeyExpr {
  def k1: KeyExpr
  def k2: KeyExpr
  override def shred: Tuple2KeyExpr = ??? //TODO
}

sealed trait Tuple3KeyExpr extends Tuple2KeyExpr {
  def k3: KeyExpr
  override def shred: Tuple3KeyExpr = ??? //TODO
}

case object UnitKeyExpr extends NullaryKeyExpr {
  val keyType = UnitType
  override def toString = "Unit"
}

case class IntKeyExpr(i: Int) extends NullaryKeyExpr {
  val keyType = IntKeyType
  override def toString = s"$i"
}

case class StringKeyExpr(s: String) extends NullaryKeyExpr {
  val keyType = StringKeyType
  override def toString = s""""$s""""
}

case class KeyPairExpr(k1: KeyExpr, k2: KeyExpr) extends Tuple2KeyExpr {
  val keyType = k1.keyType.pair(k2.keyType)
  override def toString = s"⟨$k1,$k2⟩"
  def freeVariables = k1.freeVariables ++ k2.freeVariables
  override def shred = KeyPairExpr(k1.shred, k2.shred)
}

case class KeyTuple3Expr(k1: KeyExpr, k2: KeyExpr, k3: KeyExpr) extends Tuple3KeyExpr {
  val keyType = k2.keyType.triple(k2.keyType, k3.keyType)
  override def toString = s"⟨$k2,$k2,$k3⟩"
  def freeVariables = k1.freeVariables ++ k2.freeVariables ++ k3.freeVariables
  override def shred = KeyTuple3Expr(k1.shred, k2.shred, k3.shred)
}

case class Project1KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._1
  override def toString = s"($k)._1"
  def freeVariables = k.freeVariables //TODO
  override def shred = Project1KeyExpr(k.shred)
}

case class Project2KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._2
  override def toString = s"($k)._2"
  def freeVariables = k.freeVariables //TODO
  override def shred = Project2KeyExpr(k.shred)
}

case class Project3KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._3
  override def toString = s"($k)._2"
  def freeVariables = k.freeVariables //TODO
  override def shred = Project3KeyExpr(k.shred)
}

sealed trait FreeVariable extends KeyExpr {
  def name: String
}

case class TypedFreeVariable(name: String, keyType: KeyType) extends FreeVariable {
  override def toString = s""""$name":$keyType"""
  def freeVariables = Set(this)
}

case class UntypedFreeVariable(name: String) extends FreeVariable {
  val keyType = UnresolvedKeyType
  override def toString = s""""$name":?"""
  def freeVariables = throw IllegalFreeVariableRequestException(
    "Cannot request free variables of expression with untyped free variables. Must resolve" +
      "any untyped free variables first."
  )
}

sealed trait ToK extends KeyExpr

case class BoxedRingExpr(r: RingExpr) extends ToK {
  val keyType = r.ringType.box
  override def toString = s"[$r]"
  override def freeVariables = r.freeVariables
  override def shred = LabelExpr(r.shred)
}

case class LabelExpr(r: RingExpr) extends ToK {
  val keyType = LabelType
  def freeVariables = r.freeVariables
  override def shred = throw InvalidShreddingException("Cannot shred a LabelExpr, it's already shredded.")
  override def toString = s"Label(${hashCode.toString.take(3)}"
}

case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)
case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)
case class InvalidShreddingException(msg: String) extends Exception(msg)