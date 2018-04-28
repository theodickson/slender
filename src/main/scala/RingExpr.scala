package slender

sealed trait RingExpr {
  def ringType: RingType

  def resolve(vars: Map[String,KeyType]): RingExpr //make private
  def resolve(vars: (String,KeyType)*): RingExpr = resolve(vars.toMap)
  def resolve: RingExpr = resolve(Map.empty[String,KeyType])
}

trait NullaryRingExpr extends RingExpr {
  def resolve(vars: Map[String,KeyType]) = this
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
  override def toString = s"$ref : $ringType"
}

case class Bag(keyType: KeyType, ref: String) extends FiniteMappingRingExpr with NullaryRingExpr {
  val valueType = IntType
}

case class InfMapping(key: VarKeyExpr, value: RingExpr) extends MappingRingExpr {
  val keyType = key.keyType
  val valueType = value.ringType

  def resolve(vars: Map[String, KeyType]) = {

    val newKey = vars.get(key.name) match {
      case None => key //if key is not in the resolution map, do nothing
      case Some(keyType) => key match {
        //Resolve if not already resolved:
        case UnresolvedVarKeyExpr(keyName) => ResolvedVarKeyExpr(keyName, keyType)
        //If already resolved with matching type, do nothing:
        case ResolvedVarKeyExpr(_, `keyType`) => key
        //Error if already resolved with a different type:
        case ResolvedVarKeyExpr(_, _) => throw new IllegalArgumentException("Key resolution conflict")
      }
    }

    val newValue = value.resolve(vars)

    InfMapping(newKey, newValue)

  }

  override def toString = s"{$key => $value}"
}

case class Pair(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType.pair(right.ringType)
  def resolve(vars: Map[String, KeyType]) = Pair(left.resolve(vars), right.resolve(vars))

  override def toString = s"⟨$left,$right⟩"
}

case class Project1(child: RingExpr) extends RingExpr {
  val ringType: RingType = child.ringType._1
  def resolve(vars: Map[String, KeyType]) = Project1(child.resolve(vars))
}

case class Project2(child: RingExpr) extends RingExpr {
  val ringType: RingType = child.ringType._2
  def resolve(vars: Map[String, KeyType]) = Project2(child.resolve(vars))
}

case class Plus(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType + right.ringType

  //Might want to add inferring of VarKeyExpr either on LHS or RHS
  def resolve(vars: Map[String, KeyType]) = Plus(left.resolve(vars), right.resolve(vars))

  override def toString = s"($left + $right)"
}

case class Negate(child: RingExpr) extends UnaryRingExpr {
  def resolve(vars: Map[String, KeyType]) = Negate(child.resolve(vars))
  override def toString = s"-$child"
}

case class Multiply(left: RingExpr, right: RingExpr) extends BinaryRingExpr {

  val ringType: RingType = left.ringType * right.ringType

  def resolve(vars: Map[String, KeyType]) = {
    //Resolve the LHS:
    val newLeft = left.resolve(vars)
    //To resolve the RHS, check the new LHS's ringType:
    val newRight = newLeft.ringType match {
      //If its a mapping type, we may have a new variable to resolve on the right, if it's an InfMapping:
      case MappingType(keyType,_) => right match {
        //If the RHS is indeed an InfMapping, resolve the RHS with the addition of its free variable
        //bound to the key type of the LHS:
        case InfMapping(varKeyExpr,_) => right.resolve(vars ++ Map(varKeyExpr.name -> keyType))
        //If it's not, resolve with no added vars:
        case _ => right.resolve(vars)
      }
      //If the LHS is not a mapping type, it's either unresolved or an IntType. In either case
      //resolve the RHS with no additions.
      case _ => right.resolve(vars)
    }
    Multiply(newLeft, newRight)
  }

  override def toString = s"($left * $right)"
}

case class Dot(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType dot right.ringType

  //resolving potential here?
  def resolve(vars: Map[String, KeyType]) = Dot(left.resolve(vars), right.resolve(vars))

  override def toString = s"($left ⋅ $right)"
}

case class Sum(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType.sum
  def resolve(vars: Map[String, KeyType]) = Sum(child.resolve(vars))
}

case class Predicate(keyType: KeyType)(k: KeyExpr, p: KeyExpr => IntExpr) extends RingExpr {
  val ringType: RingType = IntType
  val refs = Map.empty[String,(KeyType,RingType)] //???
  def resolve(vars: Map[String, KeyType]) = Predicate(keyType)(k.resolve(vars),p)
}

case class Not(child: RingExpr) extends UnaryRingExpr {
  def resolve(vars: Map[String, KeyType]) = Negate(child.resolve(vars))
  override def toString = s"¬($child)"
}

case class Sng(k: KeyExpr, r: RingExpr) extends MappingRingExpr {
  val keyType = k.keyType
  val valueType = r.ringType
  def resolve(vars: Map[String, KeyType]) = Sng(k.resolve(vars), r.resolve(vars))
  override def toString = s"{$k ⟼ $r}"
}

case class UnboxedVarRingExpr(k: VarKeyExpr) extends NullaryRingExpr {
  val ringType: RingType = k.keyType match {
    case UnresolvedKeyType => UnresolvedRingType
    case BoxedRingType(r) => r
    case _ => throw new IllegalArgumentException("Cannot unbox non-boxed ring variable.")
  }

  override def resolve(vars: Map[String, KeyType]) = vars.get(k.name) match {
    case None => this
    case Some(keyType) => k.keyType match {
      case UnresolvedKeyType => UnboxedVarRingExpr(ResolvedVarKeyExpr(k.name,keyType))
      case otherKeyType => if (keyType == otherKeyType) this else
        throw new IllegalArgumentException("Key variable resolution conflict.")
    }
  }

  override def toString = k.toString
}