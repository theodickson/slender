package slender

sealed trait KeyExpr {
  def keyType: KeyType
}

sealed trait NullaryKeyExpr extends KeyExpr

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
}

case class Project1KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._1
  override def toString = s"($k)._1"
}

case class Project2KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._2
  override def toString = s"($k)._2"
}

sealed trait FreeVariable extends KeyExpr {
  def name: String
}

case class TypedFreeVariable(name: String, keyType: KeyType) extends FreeVariable {
  override def toString = s""""$name":$keyType"""
}

case class UntypedFreeVariable(name: String) extends FreeVariable {
  val keyType = UnresolvedKeyType
  override def toString = s""""$name":?"""
}

sealed trait ToK extends KeyExpr

case class BoxedRingExpr(r: RingExpr) extends ToK {
  val keyType = r.ringType.box
  override def toString = s"[$r]"
}

//case class LabelExpr(r: RingExpr) extends ToK {
//
//  val keyType = LabelType
//
//  //Label based on free variables of r??
//
//
//}

case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)