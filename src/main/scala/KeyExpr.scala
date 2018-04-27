package slender

sealed trait KeyExpr {
  def keyType: KeyType
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

case class VarKeyExpr(name: String, keyType: KeyType) extends KeyExpr