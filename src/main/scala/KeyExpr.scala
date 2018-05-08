package slender

sealed trait KeyExpr extends Expr {
  def exprType: KeyType

  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): KeyExpr
  def inferTypes(vars: Map[String,ResolvedKeyType]): KeyExpr = replaceTypes(vars, false)
  def inferTypes: KeyExpr = inferTypes(Map.empty)

  def shred: KeyExpr = this match {
    case BoxedRingExpr(r) => LabelExpr(r.shred)
    case _ => shredR
  }
  def shredR: KeyExpr
}

sealed trait NullaryKeyExpr extends KeyExpr with NullaryExpr {
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = this
  def shredR: KeyExpr = this
}

sealed trait UnaryKeyExpr extends KeyExpr with UnaryExpr

sealed trait BinaryKeyExpr extends KeyExpr with BinaryExpr

sealed trait TernaryKeyExpr extends KeyExpr with TernaryExpr

sealed trait Tuple1KeyExpr extends KeyExpr with UnaryKeyExpr {
  def c1: KeyExpr
//  def shredR: Tuple1KeyExpr = ??? //TODO
}

sealed trait Tuple2KeyExpr extends KeyExpr with BinaryKeyExpr {
  def c1: KeyExpr
  def c2: KeyExpr
//  def shredR: Tuple2KeyExpr = ??? //TODO
}

sealed trait Tuple3KeyExpr extends KeyExpr with TernaryKeyExpr {
  def c1: KeyExpr
  def c2: KeyExpr
  def c3: KeyExpr
//   shredR: Tuple3KeyExpr = ??? //TODO
}

case object UnitKeyExpr extends NullaryKeyExpr {
  val exprType = UnitType
  override def toString = "Unit"
}

case class IntKeyExpr(i: Int) extends NullaryKeyExpr {
  val exprType = IntKeyType
  override def toString = s"$i"
}

case class StringKeyExpr(s: String) extends NullaryKeyExpr {
  val exprType = StringKeyType
  override def toString = s""""$s""""
}

case class KeyPairExpr(c1: KeyExpr, c2: KeyExpr) extends Tuple2KeyExpr {
  val exprType = c1.exprType.pair(c2.exprType)
  override def toString = s"⟨$c1,$c2⟩"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = KeyPairExpr(
    c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite)
  )

  def shredR = KeyPairExpr(c1.shred, c2.shred)
}

case class KeyTuple3Expr(c1: KeyExpr, c2: KeyExpr, c3: KeyExpr) extends Tuple3KeyExpr {
  val exprType = c2.exprType.triple(c2.exprType, c3.exprType)
  override def toString = s"⟨$c2,$c2,$c3⟩"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = KeyTuple3Expr(
    c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite), c3.replaceTypes(vars, overwrite)
  )

  def shredR = KeyTuple3Expr(c1.shred, c2.shred, c3.shred)
}

case class Project1KeyExpr(c1: KeyExpr) extends UnaryKeyExpr {
  val exprType = c1.exprType._1
  override def toString = c1 match {
    case TypedFreeVariable(name, kt) => s""""$name"._1"""
    case UntypedFreeVariable(name) => s""""$name._1:?"""
    case _ => s"$c1._1"
  }

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Project1KeyExpr(
    c1.replaceTypes(vars, overwrite)
  )

  def shredR = Project1KeyExpr(c1.shred)
}

case class Project2KeyExpr(c1: KeyExpr) extends UnaryKeyExpr {
  val exprType = c1.exprType._2
  override def toString = c1 match {
    case TypedFreeVariable(name, kt) => s""""$name"._2"""
    case UntypedFreeVariable(name) => s""""$name._2:?"""
    case _ => s"$c1._2"
  }

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Project2KeyExpr(
    c1.replaceTypes(vars, overwrite)
  )

  def shredR = Project2KeyExpr(c1.shred)
}

case class Project3KeyExpr(c1: KeyExpr) extends UnaryKeyExpr {
  val exprType = c1.exprType._3
  override def toString = c1 match {
    case TypedFreeVariable(name, kt) => s""""$name"._3"""
    case UntypedFreeVariable(name) => s""""$name._3:?"""
    case _ => s"$c1._3"
  }

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Project3KeyExpr(
    c1.replaceTypes(vars, overwrite)
  )

  def shredR = Project3KeyExpr(c1.shred)
}

sealed trait FreeVariable extends NullaryKeyExpr {
  def name: String
}

case class TypedFreeVariable(name: String, exprType: KeyType) extends FreeVariable {
  override def toString = s""""$name""""
  override def freeVariables = Set(this)

  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = vars.get(name) match {
    case None | Some(`exprType`) => this
    case Some(otherType) => if (overwrite) TypedFreeVariable(name, otherType) else
      throw VariableResolutionConflictException(
        s"Tried to resolve var $name with type $otherType, already had type $exprType, overwriting false."
      )
  }
}

case class UntypedFreeVariable(name: String) extends FreeVariable {
  val exprType = UnresolvedKeyType
  override def toString = s""""$name":?"""
  override def freeVariables = throw IllegalFreeVariableRequestException(
    "Cannot request free variables of expression with untyped free variables. Must resolve" +
      "any untyped free variables first."
  )

  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = vars.get(name) match {
    case None => this
    case Some(eT) => TypedFreeVariable(name, eT)
  }
}

sealed trait ToK extends UnaryKeyExpr

case class BoxedRingExpr(c1: RingExpr) extends ToK {
  val exprType = c1.exprType.box
  override def toString = s"[$c1]"

  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    BoxedRingExpr(c1.replaceTypes(vars, overwrite))

  def shredR = throw new IllegalStateException("Cannot call shredR on BoxedRingExpr")
}

case class LabelExpr(c1: RingExpr) extends ToK {
  val exprType = c1.exprType match {
    case rt: ResolvedRingType => LabelType(rt)
    case _ => throw new IllegalStateException("Cannot create labeltype from unresovled ring type.")
  }
  private def id = hashCode.abs.toString.take(3).toInt
  override def toString = s"Label($id}"
  def explain = s"$id - $c1"
  override def labelExplanations = explain +: c1.labelExplanations

  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    LabelExpr(c1.replaceTypes(vars, overwrite))

  override def shredR =
    throw InvalidShreddingException("Cannot shred a LabelExpr, it's already shredded.")
}

case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)
case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)
case class InvalidShreddingException(msg: String) extends Exception(msg)