package slender

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.collection.GenTraversableOnce

object implicits {

  implicit class DataFrameImplicits(df: DataFrame) {
    def <=>[T](data: GenTraversableOnce[T])(implicit enc: Encoder[T]): Boolean =
      df.as[T].collect.toList == data.toList

    def <~>[T](data: TraversableOnce[T])(implicit enc: Encoder[T], ord: Ordering[T]): Boolean =
      df.as[T].collect.toList.sorted == data.toList.sorted
  }

}

object dsl {

  case class UnresolvedInfMapping(varName: String, value: RingExpr)

//  implicit class StringContextImplicits(val sc: StringContext) {
//    def $(args: Any*): String = sc.parts.toList match {
//      case s::Nil => s
//      case _ => ???
//    }
//  }

  implicit class StringImplicits(s: String) {
    def <--(r: MappingRingExpr): (VarKeyExpr,MappingRingExpr) =
      (VarKeyExpr(s, r.keyType),r)
    def ==>(r: RingExpr): UnresolvedInfMapping = UnresolvedInfMapping(s, r)
  }

  implicit class RingExprImplicits(r: RingExpr) {

    def +(r1: RingExpr) = Plus(r, r1)
    def *(r1: RingExpr) = Multiply(r, r1)
    def dot(r1: RingExpr) = Dot(r, r1)

    def unary_- = Negate(r)
    def unary_! = Not(r)


    def +(m: UnresolvedInfMapping): RingExpr = r match {

      case m1 : MappingRingExpr => m.value.ringType match {
        case rT if (rT == m1.valueType) => {
          val resolvedVar = VarKeyExpr(m.varName, m1.keyType)
          r + InfMapping(resolvedVar, m.value)
        }
        case _ => throw new IllegalArgumentException("Cannot add infinite mapping of different value type.")
      }

      case _ => throw new IllegalArgumentException("Cannot add infinite mapping to non-mapping ring expression.")
    }

    def *(m: UnresolvedInfMapping): RingExpr = r match {
      case m1 : MappingRingExpr => {
        val resolved = VarKeyExpr(m.varName, m1.keyType)
        r * InfMapping(resolved, m.value)
      }
      case _ => throw new IllegalArgumentException("Cannot multiply non-mapping with infinite mapping.")
    }

  }

  implicit class MappingRingExprImplicits(r: MappingRingExpr) {
    def iff(p: Predicate): (MappingRingExpr,Predicate) = (r, p)
  }

  implicit class VarKeyExprImplicits(x: VarKeyExpr) {
    def ==>(r: RingExpr): InfMapping = InfMapping(x, r)
  }

  def sng(e: KeyExpr): Sng = Sng(e, IntExpr(1))
  def sng(e: KeyExpr, r: RingExpr) = Sng(e, r)


  //  implicit def physCollToCollSet[O[_], I](physColl: PhysicalCollection[O, I]): PhysicalCollectionSet[O] =
  //    new PhysicalCollectionSet[O](physColl)
  //
  //  implicit def physCollToCollSet[O[_]](physColls: Seq[PhysicalCollection[O, _]]): PhysicalCollectionSet[O] =
  //    new PhysicalCollectionSet[O](physColls: _*)
}
