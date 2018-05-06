package slender.dsl

import slender._

object inference {

  def inferTypes(r: RingExpr): RingExpr = inferTypesR(r, Map.empty)

  def inferTypesR(r: RingExpr, vars: Map[String,ResolvedKeyType]): RingExpr = r match {
    case _ : NullaryRingExpr => r
    case InfiniteMappingExpr(k, v) => InfiniteMappingExpr(inferTypesFreeVar(k, vars), inferTypesR(v, vars))
    case RingPairExpr(l, r) => RingPairExpr(inferTypesR(l, vars), inferTypesR(r, vars))
    case RingTuple3Expr(r1, r2, r3) =>
      RingTuple3Expr(inferTypesR(r1, vars), inferTypesR(r2, vars), inferTypesR(r3, vars))
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
    case KeyTuple3Expr(k1, k2, k3) => KeyTuple3Expr(inferTypesK(k1, vars), inferTypesK(k2, vars), inferTypesK(k3, vars))
    case Project1KeyExpr(c) => Project1KeyExpr(inferTypesK(c, vars))
    case Project2KeyExpr(c) => Project2KeyExpr(inferTypesK(c, vars))
    case Project3KeyExpr(c) => Project3KeyExpr(inferTypesK(c, vars))
    case BoxedRingExpr(r) => BoxedRingExpr(inferTypesR(r, vars))
    case UntypedFreeVariable(name) => vars.get(name) match {
      case None => k
      case Some(kT) => TypedFreeVariable(name, kT)
    }
    //TODO = is this always okay to allow resolving of already resolved variables?
    //i.e. in key nesting query, does it matter if inner for uses same var name?
    case TypedFreeVariable(name, kT) => vars.get(name) match {
      case None => k
      case Some(`kT`) => k
      case Some(otherType) => throw VariableResolutionConflictException(
        s"Tried to resolve var $name with type $otherType, already had type $kT."
      )
    }
    case LabelExpr(_) => k
  }
}
