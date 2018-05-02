package slender

sealed trait KeyExpr {
  def keyType: KeyType
  def resolveWith(vars: Map[String,KeyType]): KeyExpr
}

trait NullaryKeyExpr extends KeyExpr {
  def resolveWith(vars: Map[String,KeyType]) = this
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
  def resolveWith(vars: Map[String,KeyType]) = KeyPairExpr(l.resolveWith(vars), r.resolveWith(vars))
  override def toString = s"⟨$l,$r⟩"
}

case class Project1KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._1
  def resolveWith(vars: Map[String,KeyType]) = Project1KeyExpr(k.resolveWith(vars))
  override def toString = s"($k)._1"
}

case class Project2KeyExpr(k: KeyExpr) extends KeyExpr {
  val keyType = k.keyType._2
  def resolveWith(vars: Map[String,KeyType]) = Project2KeyExpr(k.resolveWith(vars))
  override def toString = s"($k)._2"
}

sealed trait FreeVariable extends KeyExpr {
  def name: String
  override def resolveWith(vars: Map[String,KeyType]): FreeVariable
}

case class TypedFreeVariable(name: String, keyType: KeyType) extends FreeVariable {
  //TODO = is this always okay to allow resolving of already resolved variables?
  //i.e. in key nesting query, does it matter if inner for uses same var name?
  def resolveWith(vars: Map[String,KeyType]) = vars.get(name) match {
    case None => this
    case Some(`keyType`) => this
    case Some(otherType) => throw VariableResolutionConflictException(
      s"Tried to resolve var $name with type $otherType, already had type $keyType."
    )
  }
  override def toString = s""""$name":$keyType"""
}

case class UntypedFreeVariable(name: String) extends FreeVariable {
  val keyType = UnresolvedKeyType
  def resolveWith(vars: Map[String,KeyType]) = vars.get(name) match {
    case None => this
    case Some(kT) => TypedFreeVariable(name, kT)
  }
  override def toString = s""""$name":?"""
}

sealed trait ToK extends KeyExpr

case class BoxedRingExpr(r: RingExpr) extends ToK {
  val keyType = r.ringType.box
  def resolveWith(vars: Map[String,KeyType]) = BoxedRingExpr(r.resolveWith(vars))
  override def toString = s"[$r]"
}

//case class LabelExpr(r: RingExpr) extends ToK {
//
//  val keyType = LabelType
//
//  //Label based on free variables of r??
//
//  def resolveWith(vars: Map[String,KeyType]) = throw IllegalResolutionException(
//    "Cannot attempt to resolve shredded expression. Should only shred fully resolved" +
//      "expressions"
//  )
//
//}

case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)