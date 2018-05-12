package slender

import scala.reflect.runtime.universe._

sealed trait KeyType extends ExprType {

  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (k: ResolvedKeyType, r1: ResolvedRingType) => FiniteMappingType(k,r1)
  }

  def ==>(r: RingType): RingType = (this,r) match { //todo confusing
    case (UnresolvedKeyType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (k: ResolvedKeyType, r1: ResolvedRingType) => InfiniteMappingType(k,r1)
  }

  def project(n: Int): KeyType = this match {
    case r : ProductKeyType => try r.get(n) catch {
      case _ : java.lang.IndexOutOfBoundsException =>
        throw InvalidKeyProjectionException(s"Cannot project-$n key of type $this.")
    }
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException(s"Cannot project-$n key of type $this.")
  }

  def ===(other: KeyType): RingType = (this,other) match {
    case (UnresolvedKeyType,_) | (_,UnresolvedKeyType) => UnresolvedRingType
    case _ => if (this == other) IntType else
      throw InvalidPredicateException(s"Cannot compare keys of differing type $this and $other for equality.")
  }

  def unbox: RingType = this match {
    case UnresolvedKeyType => UnresolvedRingType
    case BoxedRingType(r) => r
    case LabelType(r) => r
    case t => throw InvalidUnboxingException(s"Cannot unbox key expr of non-boxed type $t")
  }

  def shred: ResolvedKeyType

}

case object UnresolvedKeyType extends KeyType {
  override def toString = "Unresolved"
  override def isResolved = false
  def shred = ???
}

sealed trait ResolvedKeyType extends KeyType

sealed trait PrimitiveKeyType extends ResolvedKeyType {
  def tpe: Type
  def shred = this
  override def toString = tpe.toString
}

case class ProductKeyType(tts: ResolvedKeyType*) extends ResolvedKeyType with ProductExprType[ResolvedKeyType] {
  val ts = tts.toSeq
  override def toString = ts.map(_.closedString).mkString("×")
  def shred = ProductKeyType(ts.map(_.shred) : _ *)
}

case class DomKeyType(tpe: Type) extends PrimitiveKeyType

case object UnitType extends PrimitiveKeyType {
  def tpe = typeOf[Unit]
}

case object IntKeyType extends PrimitiveKeyType {
  def tpe = typeOf[Int]
}

case object StringKeyType extends PrimitiveKeyType {
  def tpe = typeOf[String]
}

case class LabelType(r: ResolvedRingType) extends ResolvedKeyType {
  override def toString = s"Label($r)"
  def shred = ???
}

case class BoxedRingType(r: ResolvedRingType) extends ResolvedKeyType {
  override def toString = s"[$r]"
  def shred = LabelType(r.shred)
}

object KeyType {
  def seq(ts: Seq[KeyType]): KeyType = if (ts.contains(UnresolvedKeyType)) UnresolvedKeyType else
    ProductKeyType(ts.map(_.asInstanceOf[ResolvedKeyType]) : _ *)
}

case class InvalidKeyProjectionException(msg: String) extends Exception(msg)
case class InvalidUnboxingException(msg: String) extends Exception(msg)