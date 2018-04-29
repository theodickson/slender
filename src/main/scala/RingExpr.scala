package slender

sealed trait RingExpr {
  def ringType: RingType
  def isResolved: Boolean = ringType != UnresolvedRingType
  def resolveWith(vars: Map[String,KeyType]): RingExpr //make private
  //def resolve(vars: (String,KeyType)*): RingExpr = resolve(vars.toMap)
  def resolve: RingExpr = resolveWith(Map.empty[String,KeyType])
}

trait NullaryRingExpr extends RingExpr {
  def resolveWith(vars: Map[String,KeyType]) = this
}

trait UnaryRingExpr extends RingExpr {
  def child: RingExpr
  def ringType: RingType = child.ringType
}

trait BinaryRingExpr extends RingExpr {
  def left: RingExpr
  def right: RingExpr
}

trait MappingRingExpr extends RingExpr {
  def keyType: KeyType
  def valueType: RingType
  def ringType: RingType = keyType --> valueType
}

trait FiniteMappingRingExpr extends MappingRingExpr

case class IntExpr(value: Int) extends NullaryRingExpr {
  val ringType = IntType
  override def toString = s"$value"
}

case class Collection(keyType: KeyType, valueType: RingType, ref: String) extends FiniteMappingRingExpr
  with NullaryRingExpr {
  override def toString = s"$ref:$ringType"
}

case class Bag(keyType: KeyType, ref: String) extends FiniteMappingRingExpr with NullaryRingExpr {
  val valueType = IntType
  override def toString = s"$ref:$ringType"
}

case class InfMapping(key: VarKeyExpr, value: RingExpr) extends MappingRingExpr {
  val keyType = key.keyType
  val valueType = value.ringType

  def resolveWith(vars: Map[String, KeyType]) = {
    val newKey = key.resolveWith(vars)
    val newValue = value.resolveWith(vars)
    InfMapping(newKey, newValue)
  }

  override def toString = s"{$key => $value}"
}

case class Pair(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType.pair(right.ringType)
  def resolveWith(vars: Map[String, KeyType]) = Pair(left.resolveWith(vars), right.resolveWith(vars))

  override def toString = s"⟨$left,$right⟩"
}

case class Project1(child: RingExpr) extends RingExpr {
  val ringType: RingType = child.ringType._1
  def resolveWith(vars: Map[String, KeyType]) = Project1(child.resolveWith(vars))
}

case class Project2(child: RingExpr) extends RingExpr {
  val ringType: RingType = child.ringType._2
  def resolveWith(vars: Map[String, KeyType]) = Project2(child.resolveWith(vars))
}

case class Add(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType + right.ringType

  //Might want to add inferring of VarKeyExpr either on LHS or RHS
  def resolveWith(vars: Map[String, KeyType]) = Add(left.resolveWith(vars), right.resolveWith(vars))

  override def toString = s"$left + $right"
}

case class Negate(child: RingExpr) extends UnaryRingExpr {
  def resolveWith(vars: Map[String, KeyType]) = Negate(child.resolveWith(vars))
  override def toString = s"-$child"
}

case class Multiply(left: RingExpr, right: RingExpr) extends BinaryRingExpr {

  val ringType: RingType = left.ringType * right.ringType

  def resolveWith(vars: Map[String, KeyType]) = {
    //Resolve the LHS:
    val newLeft = left.resolveWith(vars)
    //To resolve the RHS, check the new LHS's ringType:
    val newRight = newLeft.ringType match {
      //If its a mapping type, we may have a new variable to resolve on the right, if it's an InfMapping:
      case MappingType(keyType,_) => right match {
        //If the RHS is indeed an InfMapping, resolve the RHS with the addition of its free variable
        //bound to the key type of the LHS:
        case InfMapping(varKeyExpr,_) => right.resolveWith(vars ++ Map(varKeyExpr.name -> keyType))
        //If it's not, resolve with no added vars:
        case _ => right.resolveWith(vars)
      }
      //If the LHS is not a mapping type, it's either unresolved or an IntType. In either case
      //resolve the RHS with no additions.
      case _ => right.resolveWith(vars)
    }
    Multiply(newLeft, newRight)
  }

  override def toString = s"$left * $right"
}

case class Dot(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType dot right.ringType

  //resolving potential here?
  def resolveWith(vars: Map[String, KeyType]) = Dot(left.resolveWith(vars), right.resolveWith(vars))

  override def toString = s"$left ⋅ $right"
}

case class Sum(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType.sum
  def resolveWith(vars: Map[String, KeyType]) = Sum(child.resolveWith(vars))
}

case class Predicate(k1: KeyExpr, k2: KeyExpr) extends RingExpr {

  val ringType = k1.keyType === k2.keyType

  def resolveWith(vars: Map[String, KeyType]) = Predicate(k1.resolveWith(vars),k2.resolveWith(vars))
  override def toString = s"$k1==$k2"
}

case class Not(child: RingExpr) extends UnaryRingExpr {
  def resolveWith(vars: Map[String, KeyType]) = Negate(child.resolveWith(vars))
  override def toString = s"¬($child)"
}

case class Sng(k: KeyExpr, r: RingExpr) extends MappingRingExpr {
  val keyType = k.keyType
  val valueType = r.ringType
  def resolveWith(vars: Map[String, KeyType]) = Sng(k.resolveWith(vars), r.resolveWith(vars))
  override def toString = s"($k⟼$r)"
}

case class UnboxedVarRingExpr(k: VarKeyExpr) extends NullaryRingExpr {
  val ringType: RingType = k.keyType match {
    case UnresolvedKeyType => UnresolvedRingType
    case BoxedRingType(r) => r
    case t => throw new IllegalArgumentException(
      s"Cannot unbox non-boxed ring variable ${k.name} with type $t."
    )
  }

  override def resolveWith(vars: Map[String, KeyType]) = vars.get(k.name) match {
    case None => this
    case Some(keyType) => k.keyType match {
      case UnresolvedKeyType => UnboxedVarRingExpr(ResolvedVarKeyExpr(k.name,keyType))
      case otherKeyType => if (keyType == otherKeyType) this else
        throw new IllegalArgumentException("Key variable resolution conflict.")
    }
  }

  override def toString = k.toString
}

case class InvalidPredicateException(str: String) extends Exception(str)