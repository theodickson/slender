package slender

object dsl {

  implicit class StringImplicits(s: String) {
    def <--(r: RingExpr): (VarKeyExpr, RingExpr) = r.ringType match {
      case UnresolvedRingType => (UnresolvedVarKeyExpr(s),r)
      case MappingType(kt,_) => (ResolvedVarKeyExpr(s, kt),r)
      case _ => throw new IllegalStateException("Cannot iterate over non-mapping type RingExpr.")
    }
  }

  implicit class StringContextImplicits(sc: StringContext) {
    def v(args: Any*): UnresolvedVarKeyExpr = {
      assert(args.isEmpty)
      assert(sc.parts.size == 1)
      UnresolvedVarKeyExpr(sc.parts.head)
    }
  }

  implicit def stringToUnresolvedVarKeyExpr(s: String): UnresolvedVarKeyExpr = UnresolvedVarKeyExpr(s)

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)

  implicit class RingExprImplicits(r: RingExpr) {
    def +(r1: RingExpr) = Plus(r, r1)
    def *(r1: RingExpr) = Multiply(r, r1)
    def dot(r1: RingExpr) = Dot(r, r1)
    def unary_- = Negate(r)
    def unary_! = Not(r)

    def _1: RingExpr = Project1(r)
    def _2: RingExpr = Project2(r)
  }

  implicit class KeyExprImplicits(k: KeyExpr) {
    def _1: KeyExpr = Project1K(k)
    def _2: KeyExpr = Project2K(k)
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
  implicit def fromK(k: BoxedRingExpr): RingExpr = k.r
  implicit def fromK(s: String): RingExpr = UnboxedVarRingExpr(UnresolvedVarKeyExpr(s))


  case class ForComprehensionBuilder(x: VarKeyExpr, r1: RingExpr) {

    def Collect(r2: RingExpr): RingExpr = Sum(r1 * {x ==> r2}).resolve

    def Yield(pair: (KeyExpr, RingExpr)): RingExpr = Collect(sng(pair._1, pair._2))

    def Yield(k: KeyExpr): RingExpr = Collect(sng(k))

  }


  case class PredicatedForComprehensionBuilder(x: VarKeyExpr, exprPred: (RingExpr, Predicate)) {

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
    def apply(paired: (VarKeyExpr, RingExpr)): ForComprehensionBuilder =
      ForComprehensionBuilder(paired._1, paired._2)
  }

  case class PredicatedMappingRingExpr(m: RingExpr, p: Predicate)

}