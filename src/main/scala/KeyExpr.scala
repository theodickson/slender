package slender

//import definitions._
//
//sealed trait KeyExpr extends Expr {
//  def exprType: KeyType
//
//  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): KeyExpr
//  def inferTypes(vars: Map[String,ResolvedKeyType]): KeyExpr = replaceTypes(vars, false)
//  def inferTypes: KeyExpr = inferTypes(Map.empty)
//
//  def shred: KeyExpr = this match {
//    case BoxedRingExpr(r) => LabelExpr(r.shred)
//    case _ => shredR
//  }
//  def shredR: KeyExpr
//}
//
//sealed trait NullaryKeyExpr extends KeyExpr with NullaryExpr {
//  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = this
//  def shredR: KeyExpr = this
//}
//
//sealed trait UnaryKeyExpr extends KeyExpr with UnaryExpr
//
//sealed trait BinaryKeyExpr extends KeyExpr with BinaryExpr
//
//sealed trait TernaryKeyExpr extends KeyExpr with TernaryExpr
//
//case object UnitKeyExpr extends NullaryKeyExpr {
//  val exprType = UnitKeyType
//  override def toString = "Unit"
//}
//
//case class IntKeyExpr(i: Int) extends NullaryKeyExpr {
//  val exprType = IntKeyType
//  override def toString = s"$i"
//}
//
//case class StringKeyExpr(s: String) extends NullaryKeyExpr {
//  val exprType = StringKeyType
//  override def toString = s""""$s""""
//}
//
//case class KeyProductExpr(cs: KeyExpr*) extends KeyExpr {
//  val children = cs.toSeq
//  val exprType = KeyType.seq(children.map(_.exprType))
//  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
//    KeyProductExpr(children.map(_.replaceTypes(vars, overwrite)) : _ *)
//  def shredR = KeyProductExpr(children.map(_.shred) : _ *)
//  override def toString = s"⟨${children.mkString(",")}⟩"
//}
//
//case class ProjectKeyExpr(c1: KeyExpr, n: Int) extends UnaryKeyExpr {
//  val exprType = c1.exprType.project(n)
//  override def toString = c1 match {
//    case TypedVariableKeyExpr(name, kt) => s""""$name"._$n"""
//    case UntypedVariableKeyExpr(name) => s""""$name._$n:?"""
//    case _ => s"$c1._$n"
//  }
//
//  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
//    ProjectKeyExpr(c1.replaceTypes(vars, overwrite), n)
//
//  def shredR = ProjectKeyExpr(c1.shred, n)
//}
//
//sealed trait VariableKeyExpr extends NullaryKeyExpr {
//  def name: String
//  override def variables = Set(this)
//  override def freeVariables = Set(this)
//}
//
//case class TypedVariableKeyExpr(name: String, exprType: KeyType) extends VariableKeyExpr {
//  override def toString = s""""$name""""
//  override def explain: String = s""""$name": $exprType"""
//  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = vars.get(name) match {
//    case None | Some(`exprType`) => this
//    case Some(otherType) => if (overwrite) TypedVariableKeyExpr(name, otherType) else
//      throw VariableResolutionConflictException(
//        s"Tried to resolve var $name with type $otherType, already had type $exprType, overwriting false."
//      )
//  }
//}
//
//case class UntypedVariableKeyExpr(name: String) extends VariableKeyExpr {
//  val exprType = UnresolvedKeyType
//  override def explain = toString
//  override def toString = s""""$name": ?"""
//  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = vars.get(name) match {
//    case None => this
//    case Some(eT) => TypedVariableKeyExpr(name, eT)
//  }
//}
//
//sealed trait ToK extends UnaryKeyExpr
//
//case class BoxedRingExpr(c1: RingExpr) extends ToK {
//  val exprType = c1.exprType.box
//  override def toString = s"[$c1]"
//
//  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
//    BoxedRingExpr(c1.replaceTypes(vars, overwrite))
//
//  def shredR = throw new IllegalStateException("Cannot call shredR on BoxedRingExpr")
//}
//
//case class LabelExpr(c1: RingExpr) extends ToK {
//
//  val exprType = c1.exprType match {
//    case rt: ResolvedRingType => LabelType(rt)
//    case _ => throw new IllegalStateException("Cannot create labeltype from unresolved ring type.")
//  }
//
//  private def id = hashCode.abs.toString.take(3).toInt
//
//  override def toString = s"Label($id)"
//
//  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")
//
//  def definition: String = {
//    val s = new StringBuilder(s"$this: ${c1.exprType}")
//    s ++= s"\n\n\t$c1"
//    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
////    s ++= s"\n\tWrapped expr:\n\t$c1"
//    s.mkString
//  }
//  override def labelDefinitions = c1.labelDefinitions :+ definition
//
//  override def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
//    LabelExpr(c1.replaceTypes(vars, overwrite))
//
//  override def shredR =
//    throw InvalidShreddingException("Cannot shred a LabelExpr, it's already shredded.")
//
//  override def isShredded = true
//}
//
//case class VariableResolutionConflictException(msg: String) extends Exception(msg)
//case class IllegalResolutionException(msg: String) extends Exception(msg)
//case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)
//case class InvalidShreddingException(msg: String) extends Exception(msg)