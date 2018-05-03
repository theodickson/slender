package slender

sealed trait KeyExpr {
  def keyType: KeyType
  def freeVariables: Set[TypedFreeVariable]
}

sealed trait NullaryKeyExpr extends KeyExpr {
  def freeVariables = Set.empty
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

case class KeyPairExpr(l: KeyExpr, r: KeyExpr) extends KeyExpr {
  val keyType = l.keyType.pair(r.keyType)
  override def toString = s"⟨$l,$r⟩"
  def freeVariables = l.freeVariables ++ r.freeVariables
}

case class Project1KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._1
  override def toString = s"($k)._1"
  def freeVariables = k.freeVariables //TODO
}

case class Project2KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._2
  override def toString = s"($k)._2"
  def freeVariables = k.freeVariables //TODO
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
}

case class LabelExpr(r: RingExpr) extends ToK {
  val keyType = LabelType
  def freeVariables = r.freeVariables
}

case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)
case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)