package slender

object dsl {

  case class UnresolvedInfMapping(varName: String, value: RingExpr)

  implicit class StringImplicits(s: String) {
    def <--(r: MappingRingExpr): (VarKeyExpr, MappingRingExpr) =
      (ResolvedVarKeyExpr(s, r.keyType), r)

    def ==>(r: RingExpr): UnresolvedInfMapping = UnresolvedInfMapping(s, r)
  }

  implicit class RingExprImplicits(r: RingExpr) {

    def +(r1: RingExpr) = Plus(r, r1)

    def *(r1: RingExpr) = Multiply(r, r1)

    def dot(r1: RingExpr) = Dot(r, r1)

    def unary_- = Negate(r)

    def unary_! = Not(r)


    def +(m: UnresolvedInfMapping): RingExpr = r match {

      case m1: MappingRingExpr => m.value.ringType match {
        case rT if (rT == m1.valueType) => {
          val resolvedVar = ResolvedVarKeyExpr(m.varName, m1.keyType)
          r + InfMapping(resolvedVar, m.value)
        }
        case _ => throw new IllegalArgumentException("Cannot add infinite mapping of different value type.")
      }

      case _ => throw new IllegalArgumentException("Cannot add infinite mapping to non-mapping ring expression.")
    }

    def *(m: UnresolvedInfMapping): RingExpr = r match {
      case m1: MappingRingExpr => {
        val resolved = ResolvedVarKeyExpr(m.varName, m1.keyType)
        r * InfMapping(resolved, m.value)
      }
      case _ => throw new IllegalArgumentException("Cannot multiply non-mapping with infinite mapping.")
    }

  }

  implicit class MappingRingExprImplicits(r: MappingRingExpr) {
    def iff(p: Predicate): (MappingRingExpr, Predicate) = (r, p)
  }

  implicit class VarKeyExprImplicits(x: VarKeyExpr) {
    def ==>(r: RingExpr): InfMapping = InfMapping(x, r)
  }

  def sng(e: KeyExpr): Sng = Sng(e, IntExpr(1))

  def sng(e: KeyExpr, r: RingExpr) = Sng(e, r)

  def toK(r: RingExpr): KeyExpr = BoxedRingExpr(r)

  def fromK(k: BoxedRingExpr): RingExpr = k.r

  case class ForComprehensionBuilder(x: VarKeyExpr, r1: MappingRingExpr) {
    def Collect(r2: RingExpr): RingExpr = Sum(r1 * {
      x ==> r2
    })

    def Yield(pair: (KeyExpr, RingExpr)): RingExpr = Collect(sng(pair._1, pair._2))

    def Yield(k: KeyExpr): RingExpr = Collect(sng(k))
  }

  case class PredicatedForComprehensionBuilder(x: VarKeyExpr, exprPred: (MappingRingExpr, Predicate)) {

    val r1 = exprPred._1
    val p = exprPred._2
    val builder = ForComprehensionBuilder(x, r1)

    def Yield(keyRing: (KeyExpr, RingExpr)): RingExpr = {
      val keyExpr = keyRing._1
      val ringExpr = keyRing._2
      builder.Yield((keyExpr, p dot ringExpr))
    }

    def Yield(k: KeyExpr): RingExpr = builder.Yield((k, p))
  }

  object For {
    def apply(paired: (VarKeyExpr, MappingRingExpr)): ForComprehensionBuilder =
      ForComprehensionBuilder(paired._1, paired._2)
  }

  case class PredicatedMappingRingExpr(m: MappingRingExpr, p: Predicate)

}