package slender

//TODO - resolution inc unresolved type handling

sealed trait KeyExpr {
  def keyType: KeyType
  def resolve
}

case object UnitKeyExpr extends KeyExpr {
  def keyType = UnitType
}

case class IntKeyExpr(i: Int) extends KeyExpr {
  def keyType = DomIntType
}

case class StringKeyExpr(s: String) extends KeyExpr {
  def keyType = DomStringType
}

case class KeyPairExpr(l: KeyExpr, r: KeyExpr) extends KeyExpr {
  def keyType = l.keyType.pair(r.keyType)
}

case class BoxedRingExpr(r: RingExpr) extends KeyExpr {
  def keyType = BoxedRing(r.ringType)
}

sealed trait VarKeyExpr extends KeyExpr {
  def name: String
}

case class ResolvedVarKeyExpr(name: String, keyType: KeyType) extends VarKeyExpr

case class UnresolvedVarKeyExpr(name: String) extends VarKeyExpr {
  def keyType = UnresolvedKeyType
}
