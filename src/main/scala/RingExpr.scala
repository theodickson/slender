package slender

sealed trait RingExpr {
  def ringType: RingType
  def isResolved: Boolean = ringType != UnresolvedRingType
  def resolveWith(vars: Map[String,KeyType]): RingExpr
  def resolve: RingExpr = resolveWith(Map.empty[String,KeyType])

//  def freeVariables: Set[TypedFreeVariable] = ???
//
//  def shred: ShreddedRingExpr = ???
}

sealed trait NullaryRingExpr extends RingExpr {
  def resolveWith(vars: Map[String,KeyType]) = this
}

sealed trait UnaryRingExpr extends RingExpr {
  def child: RingExpr
  def ringType: RingType = child.ringType
//  def resolveWith(vars: Map[String,KeyType]): this.type = this(child.resolveWith(vars))
}

sealed trait BinaryRingExpr extends RingExpr {
  def left: RingExpr
  def right: RingExpr
}

sealed trait MappingExpr extends RingExpr {
  def keyType: KeyType
  def valueType: RingType
  def ringType: RingType = keyType --> valueType //TODO - could hide errors by not being called on init
}

sealed trait PhysicalMapping extends MappingExpr with NullaryRingExpr

case class IntExpr(value: Int) extends NullaryRingExpr {
  val ringType = IntType
  override def toString = s"$value"
}

case class PhysicalCollection(keyType: KeyType, valueType: RingType, ref: String) extends PhysicalMapping {
  override def toString = s"$ref:$ringType"
}

case class PhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def toString = s"$ref:$ringType"
}

case class InfiniteMappingExpr(key: FreeVariable, value: RingExpr) extends MappingExpr {
//  assert(key.isInstanceOf[FreeVariable]) //TODO
  val keyType = key.keyType
  val valueType = value.ringType

  def resolveWith(vars: Map[String, KeyType]) = {
    val newKey = key.resolveWith(vars)
    val newValue = value.resolveWith(vars)
    InfiniteMappingExpr(newKey, newValue)
  }

  override def toString = s"{$key => $value}"
}

case class RingPairExpr(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType.pair(right.ringType)
  def resolveWith(vars: Map[String, KeyType]) = RingPairExpr(left.resolveWith(vars), right.resolveWith(vars))

  override def toString = s"⟨$left,$right⟩"
}

case class Project1RingExpr(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType._1
  def resolveWith(vars: Map[String, KeyType]) = Project1RingExpr(child.resolveWith(vars))
}

case class Project2RingExpr(child: RingExpr) extends RingExpr {
  val ringType: RingType = child.ringType._2
  def resolveWith(vars: Map[String, KeyType]) = Project2RingExpr(child.resolveWith(vars))
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
        case InfiniteMappingExpr(varKeyExpr,_) => right.resolveWith(vars ++ Map(varKeyExpr.name -> keyType))
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

case class Sng(k: KeyExpr, r: RingExpr) extends MappingExpr {
  val keyType = k.keyType
  val valueType = r.ringType
  def resolveWith(vars: Map[String, KeyType]) = Sng(k.resolveWith(vars), r.resolveWith(vars))
  override def toString = s"($k⟼$r)"
}

sealed trait FromK extends RingExpr

case class FromBoxedRing(k: KeyExpr) extends FromK {
  val ringType: RingType = k.keyType.unbox
  override def resolveWith(vars: Map[String, KeyType]) = FromBoxedRing(k.resolveWith(vars))
}

//case class FromLabel(l: Label, ctx: ShreddingContext) extends FromK

case class InvalidPredicateException(str: String) extends Exception(str)