package slender

object dsl {

  implicit class StringImplicits(s: String) {

    def <--(r: RingExpr): (FreeVariable, RingExpr) = (UntypedFreeVariable(s),r)
    def -->(r: RingExpr): YieldPair = YieldPair(UntypedFreeVariable(s),r)
    //since implicit methods on result of implicit conversions dont tend to resolve,
    //duplicate keyexpr implicits here:
    def _1: KeyExpr = Project1KeyExpr(s)
    def _2: KeyExpr = Project2KeyExpr(s)

    def ===(other: KeyExpr): RingExpr = Predicate(s, other)
    def ===(other: String): RingExpr = Predicate(s, other)
    def =!=(other: KeyExpr): RingExpr = Not(Predicate(s,other))
    def =!=(other: String): RingExpr = Not(Predicate(s,other))

    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(s, r)
  }

  implicit def stringToUnresolvedVarKeyExpr(s: String): UntypedFreeVariable = UntypedFreeVariable(s)

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)

  implicit def intToIntKeyExpr(i: Int): IntKeyExpr = IntKeyExpr(i)

  implicit def ringExprPairImplicits(p: (RingExpr,RingExpr)): RingPairExpr = RingPairExpr(p._1,p._2)

  implicit def keyExprPairImplicits(p: (KeyExpr,KeyExpr)): KeyPairExpr = KeyPairExpr(p._1,p._2)

  implicit def ringTypePairImplicits(p: (ResolvedRingType,ResolvedRingType)): ResolvedRingType =
    RingPairType(p._1, p._2)

  implicit def keyTypePairImplicits(p: (ResolvedKeyType,ResolvedKeyType)): ResolvedKeyType =
    KeyPairType(p._1, p._2)

  implicit def box(r: ResolvedRingType): KeyType = r.box

  implicit class RingExprImplicits(r: RingExpr) {
    def +(r1: RingExpr) = Add(r, r1)
    def *(r1: RingExpr) = Multiply(r, r1)

    def dot(r1: RingExpr) = Dot(r, r1)
    def unary_- = Negate(r)
    def unary_! = Not(r)

    def _1: RingExpr = Project1RingExpr(r)
    def _2: RingExpr = Project2RingExpr(r)

    def && = * _
    def || = this.+ _

  }

  implicit class KeyExprImplicits(k: KeyExpr) {
    def _1: KeyExpr = Project1KeyExpr(k)
    def _2: KeyExpr = Project2KeyExpr(k)

    def ===(other: KeyExpr): RingExpr = Predicate(k, other)
    def ===(other: String): RingExpr = Predicate(k, other)
    def =!=(other: KeyExpr): RingExpr = Not(Predicate(k,other))
    def =!=(other: String): RingExpr = Not(Predicate(k, other))

    def -->(r: RingExpr): YieldPair = YieldPair(k,r)
  }

  implicit class IffImplicit(pair: (FreeVariable,RingExpr)) {
    def iff(r1: RingExpr): (FreeVariable,(RingExpr,RingExpr)) = (pair._1,(pair._2,r1))
  }

  implicit class VarKeyExprImplicits(x: FreeVariable) {
    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(x, r)
  }

  def sng(e: KeyExpr): Sng = Sng(e, IntExpr(1))
  def sng(e: KeyExpr, r: RingExpr) = Sng(e, r)
  implicit def toK(r: RingExpr): KeyExpr = BoxedRingExpr(r)
  implicit def fromK(k: BoxedRingExpr): RingExpr = k.r
  implicit def fromK(s: String): RingExpr = FromBoxedRing(UntypedFreeVariable(s))


  case class ForComprehensionBuilder(x: FreeVariable, r1: RingExpr) {

    def Collect(r2: RingExpr): RingExpr = inferTypes(Sum(r1 * {x ==> r2}))

    def Yield(pair: YieldPair): RingExpr = Collect(sng(pair.k, pair.r))

    def Yield(k: KeyExpr): RingExpr = Collect(sng(k))

  }


  case class PredicatedForComprehensionBuilder(x: FreeVariable, exprPred: (RingExpr, RingExpr)) {

    val r1 = exprPred._1
    val p = exprPred._2
    //Check if pred is int typed? (probably not)
    val builder = ForComprehensionBuilder(x, r1)

    def Yield(pair: YieldPair): RingExpr =
      builder.Yield(pair.k --> (p dot pair.r))

    def Yield(k: KeyExpr): RingExpr = builder.Yield(k --> p)

  }

  object For {
    def apply(paired: (FreeVariable, RingExpr)): ForComprehensionBuilder =
      ForComprehensionBuilder(paired._1, paired._2)

    def apply(paired: (FreeVariable, (RingExpr,RingExpr))): PredicatedForComprehensionBuilder =
      PredicatedForComprehensionBuilder(paired._1, paired._2)
  }


  def inferTypes(r: RingExpr): RingExpr = {

    def inferTypesFreeVar(v: FreeVariable, vars: Map[String,ResolvedKeyType]): FreeVariable = v match {
      case UntypedFreeVariable(name) => vars.get(name) match {
        case None => v
        case Some(kT) => TypedFreeVariable(name, kT)
      }
      case TypedFreeVariable(name, kT) => vars.get(name) match {
        case None => v
        case Some(`kT`) => v
        case Some(otherType) => throw VariableResolutionConflictException(
          s"Tried to resolve var $name with type $otherType, already had type $kT."
        )
      }
    }

    def inferTypesK(k: KeyExpr, vars: Map[String,ResolvedKeyType]): KeyExpr = k match {
      case _ : NullaryKeyExpr => k
      case KeyPairExpr(l, r) => KeyPairExpr(inferTypesK(l, vars), inferTypesK(r, vars))
      case Project1KeyExpr(c) => Project1KeyExpr(inferTypesK(c, vars))
      case Project2KeyExpr(c) => Project2KeyExpr(inferTypesK(c, vars))
      //case _ : FreeVariable => throw new IllegalStateException("Use inferTypesFreeVar for free variables.")
      case BoxedRingExpr(r) => BoxedRingExpr(inferTypesR(r, vars))
      case UntypedFreeVariable(name) => vars.get(name) match {
        case None => k
        case Some(kT) => TypedFreeVariable(name, kT)
      }
      case TypedFreeVariable(name, kT) => vars.get(name) match {
        case None => k
        case Some(`kT`) => k
        case Some(otherType) => throw VariableResolutionConflictException(
          s"Tried to resolve var $name with type $otherType, already had type $kT."
        )
      }
    }

    def inferTypesR(r: RingExpr, vars: Map[String,ResolvedKeyType]): RingExpr = r match {
      case _ : NullaryRingExpr => r
      case InfiniteMappingExpr(k, v) => InfiniteMappingExpr(inferTypesFreeVar(k, vars), inferTypesR(v, vars))
      case RingPairExpr(l, r) => RingPairExpr(inferTypesR(l, vars), inferTypesR(r, vars))
      case Project1RingExpr(p) => Project1RingExpr(inferTypesR(p, vars))
      case Project2RingExpr(p) => Project2RingExpr(inferTypesR(p, vars))
      case Add(l, r) => Add(inferTypesR(l, vars), inferTypesR(r, vars))
      case Negate(c) => Negate(inferTypesR(c, vars))
      case Dot(l, r) => Dot(inferTypesR(l, vars), inferTypesR(r, vars))
      case Sum(c) => Sum(inferTypesR(c, vars))
      case Predicate(k1, k2) => Predicate(inferTypesK(k1, vars), inferTypesK(k2, vars))
      case Not(c) => Not(inferTypesR(c, vars))
      case Sng(k, r) => Sng(inferTypesK(k, vars), inferTypesR(r, vars))
      case FromBoxedRing(k) => FromBoxedRing(inferTypesK(k, vars))


      case Multiply(l, r) => {
        //Resolve the LHS:
        val newLeft = inferTypesR(l, vars)
        //To resolve the RHS, check the new LHS's ringType:
        val newRight = newLeft.ringType match {
          //If its a mapping type, we may have a new variable to resolve on the right, if it's an InfMapping:
          case MappingType(keyType,_) => r match {
            //If the RHS is indeed an InfMapping, resolve the RHS with the addition of its free variable
            //bound to the key type of the LHS:
            case InfiniteMappingExpr(v,_) => inferTypesR(r, vars ++ Map(v.name -> keyType))
           // case InfiniteMappingExpr(_,_) => throw new IllegalStateException
            //If it's not, resolve with no added vars:
            case _ => inferTypesR(r, vars)
          }
          //If the LHS is not a mapping type, it's either unresolved or an IntType. In either case
          //resolve the RHS with no additions.
          case _ => inferTypesR(r, vars)
        }
        Multiply(newLeft, newRight)
      }
    }

    inferTypesR(r, Map.empty)
  }

}

case class YieldPair(k: KeyExpr, r: RingExpr)