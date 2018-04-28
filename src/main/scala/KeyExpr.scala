package slender

sealed trait KeyExpr {
  def keyType: KeyType
  def resolve(vars: Map[String,KeyType]): KeyExpr
}

trait NullaryKeyExpr extends KeyExpr {
  def resolve(vars: Map[String,KeyType]): KeyExpr = this
}

case object UnitKeyExpr extends NullaryKeyExpr {
  val keyType = UnitType
  override def toString = "Unit"
}

case class IntKeyExpr(i: Int) extends NullaryKeyExpr {
  val keyType = DomIntType
  override def toString = s"$i"
}

case class StringKeyExpr(s: String) extends NullaryKeyExpr {
  val keyType = DomStringType
  override def toString = s""""$s""""
}

case class KeyPairExpr(l: KeyExpr, r: KeyExpr) extends KeyExpr {
  val keyType = l.keyType.pair(r.keyType)
  def resolve(vars: Map[String,KeyType]): KeyExpr = KeyPairExpr(l.resolve(vars), r.resolve(vars))
  override def toString = s"⟨$l,$r⟩"
}

case class Project1K(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._1
  def resolve(vars: Map[String,KeyType]): KeyExpr = Project1K(k.resolve(vars))
}

case class Project2K(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._2
  def resolve(vars: Map[String,KeyType]): KeyExpr = Project2K(k.resolve(vars))
}

case class BoxedRingExpr(r: RingExpr) extends KeyExpr {
  val keyType = BoxedRingType(r.ringType)
  def resolve(vars: Map[String,KeyType]): KeyExpr = BoxedRingExpr(r.resolve(vars))
  override def toString = s"[$r]"
}

sealed trait VarKeyExpr extends KeyExpr {
  def name: String
}

case class ResolvedVarKeyExpr(name: String, keyType: KeyType) extends VarKeyExpr {
  def resolve(vars: Map[String,KeyType]): KeyExpr = vars.get(name) match {
    case None => this
    case Some(`keyType`) => this
    case Some(_) => throw new IllegalArgumentException("Key type clash in var resolution.")
  }
  override def toString = s"""("$name":$keyType)"""
}

case class UnresolvedVarKeyExpr(name: String) extends VarKeyExpr {
  val keyType = UnresolvedKeyType
  def resolve(vars: Map[String,KeyType]): KeyExpr = vars.get(name) match {
    case None => this
    case Some(kT) => ResolvedVarKeyExpr(name, kT)
  }
  override def toString = s""""$name":?"""
}
