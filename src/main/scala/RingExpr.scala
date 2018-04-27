package slender
import scala.reflect.Manifest

//TODO - equality checks with refs

sealed trait RingExpr {
  def ringType: RingType
  def refs: Map[String,(KeyType,RingType)]

  def resolve(vars: Map[String,KeyType]): RingExpr
  def resolve: RingExpr = resolve(Map.empty[String,KeyType])
  //def resolveWith(varName: String, keyType)
  //free var check?
}

trait NullaryRingExpr extends RingExpr {
  def refs = Map.empty[String,(KeyType,RingType)]
  def resolve(vars: Map[String,KeyType]) = this
}

trait UnaryRingExpr extends RingExpr {
  def child: RingExpr
  def ringType: RingType = child.ringType
  def refs = child.refs
}

trait BinaryRingExpr extends RingExpr {
  def left: RingExpr
  def right: RingExpr
  def refs = left.refs ++ right.refs
}

trait MappingRingExpr extends RingExpr {
  def keyType: KeyType
  def valueType: RingType
  def ringType: RingType = MappingType(keyType, valueType)
}

trait FiniteMappingRingExpr extends MappingRingExpr

case class IntExpr(value: Int) extends NullaryRingExpr {
  val ringType = IntType
}

case class Collection(keyType: KeyType, valueType: RingType, ref: String) extends FiniteMappingRingExpr
  with NullaryRingExpr {
  override val refs = Map(ref -> (keyType,ringType))
}

case class Bag(keyType: KeyType, ref: String) extends FiniteMappingRingExpr with NullaryRingExpr {
  val valueType = IntType
  override val refs = Map(ref -> (keyType,ringType))
}

case class InfMapping(key: VarKeyExpr, value: RingExpr) extends MappingRingExpr {
  //checks on vars in ring expr?
  val keyType = key.keyType
  val valueType = value.ringType
  val refs = value.refs //Possibly not always true and quite a problem if so

  def resolve(vars: Map[String, KeyType]) = {

    val newKey = vars.get(key.name) match {
      case None => key //if key is not in the resolution map, do nothing
      case Some(keyType) => key match {
        //Resolve if not already resolved:
        case UnresolvedVarKeyExpr(keyName) => ResolvedVarKeyExpr(keyName, keyType)
        //If already resolved with matching type, do nothing:
        case ResolvedVarKeyExpr(keyName, `keyType`) => key
        //Error if already resolved with a different type:
        case ResolvedVarKeyExpr(_, _) => throw new IllegalArgumentException("Key resolution conflict")
      }
    }

    val newValue = value.resolve(vars)

    InfMapping(newKey, newValue)

  }
}

case class Pair(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = RingPairType(left.ringType, right.ringType)

  def resolve(vars: Map[String, KeyType]) = Pair(left.resolve(vars), right.resolve(vars))
}

case class Project1(p: Pair) extends RingExpr {
  val ringType: RingType = p.left.ringType
  val refs = p.left.refs

  def resolve(vars: Map[String, KeyType]) = Project1(p.resolve(vars))
}

case class Project2(p: Pair) extends RingExpr {
  val ringType: RingType = p.right.ringType
  val refs = p.right.refs

  def resolve(vars: Map[String, KeyType]) = Project2(p.resolve(vars))
}

case class Plus(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType + right.ringType

  //Might want to add inferring of VarKeyExpr either on LHS or RHS
  def resolve(vars: Map[String, KeyType]) = Plus(left.resolve(vars), right.resolve(vars))
}

case class Negate(child: RingExpr) extends UnaryRingExpr {
  def resolve(vars: Map[String, KeyType]) = Negate(child.resolve(vars))
}

case class Multiply(left: RingExpr, right: RingExpr) extends BinaryRingExpr {

  val ringType: RingType = left.ringType * right.ringType

  def resolve(vars: Map[String, KeyType]) = {
    //Resolve the LHS:
    val newLeft = left.resolve(vars)
    //To resolve the RHS, check the new LHS's ringType:
    val newRight = newLeft.ringType match {
      //If still unresolved, resolve the right with no added vars
      case UnresolvedRingType => right.resolve(vars)
      //If its resolved, we may have a new variable to resolve on the right:
      case MappingType(keyType,valueType) => right match {
        //If the right is indeed an InfMapping, resolve its key using the keyType of the LHS:
        case InfMapping(varKeyExpr,value) => right.resolve(vars ++ Map(varKeyExpr.name -> keyType))
        //If it's not, resolve with no added vars:
        case _ => right.resolve(vars)
      }
    }
    Multiply(newLeft, newRight)
  }
}

case class Dot(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType dot right.ringType

  //resolving potential here?
  def resolve(vars: Map[String, KeyType]) = Dot(left.resolve(vars), right.resolve(vars))
}

case class Sum(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType.sum
  def resolve(vars: Map[String, KeyType]) = Sum(child.resolve(vars))
}

case class Predicate(keyType: KeyType)(k: KeyExpr, p: KeyExpr => IntExpr) extends RingExpr {
  val ringType: RingType = IntType
  val refs = Map.empty[String,(KeyType,RingType)] //???
  def resolve(vars: Map[String, KeyExpr]) = Predicate(keyType)(k.resolve(vars),p)
}

case class Not(child: RingExpr) extends UnaryRingExpr {
  def resolve(vars: Map[String, KeyType]) = Negate(child.resolve(vars))
}

case class Sng(k: KeyExpr, r: RingExpr) extends MappingRingExpr {
  val keyType = k.keyType
  val valueType = r.ringType
  val refs = r.refs //???
  def resolve(vars: Map[String, KeyType]) = Sng(k.resolve(vars), r.resolve(vars))
}

case class UnboxedVarRingExpr(k: VarKeyExpr) extends NullaryRingExpr {
  val ringType: RingType = k.keyType match {
    case UnresolvedKeyType => UnresolvedRingType
    case BoxedRing(r) => r
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
}







//  //probably dont want this here
//  def assertPhysicalCollectionsValid[T](physicalCollections: PhysicalCollection[T]*): Unit = {
//    val collectionsMap = physicalCollections.map(p => (p.ref,p)).toMap //check distinct?
//    //First check all needed refs appear in the given physical collections, and that the inner types of the
//    //respective inner collections match those needed by the ref in the RingExpr.
//    refs foreach { case (ref,typ) =>
//      collectionsMap.get(ref) match {
//        case None => throw new IllegalArgumentException(
//          s"Ref $ref does not appear in the given physical collections"
//        )
//        case Some(physColl) => if (physColl.collectionType != typ)
//          throw new IllegalArgumentException(
//            s"Ref $ref has different types in the physical collection and the ring expression."
//          )
//      }
//    }
//    //    //Then check that all outer types (E.g. RDD, Dataset, List, etc) of the physical collections are the same
//    //    //Possibly unnecessary
//    //    val headOuterTag = physicalCollections.head.outerTag
//    //    physicalCollections foreach { p =>
//    //      if (p.outerTag != headOuterTag)
//    //        throw new IllegalArgumentException("Not all physical collections have the same type.")
//    //    }
//  }


//Alternative is
//sealed trait RingExpr[R <: RingExpr[R]] {
//  def ringType: RingType
//  def refs: Map[String,(KeyType,RingType)]
//
//  //probably dont want this here
//  def assertPhysicalCollectionsValid[T](physicalCollections: PhysicalCollection[T]*): Unit = {
//    val collectionsMap = physicalCollections.map(p => (p.ref,p)).toMap //check distinct?
//    //First check all needed refs appear in the given physical collections, and that the inner types of the
//    //respective inner collections match those needed by the ref in the RingExpr.
//    refs foreach { case (ref,typ) =>
//      collectionsMap.get(ref) match {
//        case None => throw new IllegalArgumentException(
//          s"Ref $ref does not appear in the given physical collections"
//        )
//        case Some(physColl) => if (physColl.collectionType != typ)
//          throw new IllegalArgumentException(
//            s"Ref $ref has different types in the physical collection and the ring expression."
//          )
//      }
//    }
//    //    //Then check that all outer types (E.g. RDD, Dataset, List, etc) of the physical collections are the same
//    //    //Possibly unnecessary
//    //    val headOuterTag = physicalCollections.head.outerTag
//    //    physicalCollections foreach { p =>
//    //      if (p.outerTag != headOuterTag)
//    //        throw new IllegalArgumentException("Not all physical collections have the same type.")
//    //    }
//  }
//}
//
//
//case class SimpleCollection(keyType: KeyType, ref: String) extends RingExpr[SimpleCollection] {
//  val ringType = IntType
//  val refs = Map(ref -> (keyType,ringType))
//}
//
//case class Collection(keyType: KeyType, ringType: RingType, ref: String) extends RingExpr[Collection] {
//  val refs = Map(ref -> (keyType,ringType))
//}
//
//case class Pair[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Pair[L, R]] {
//  val ringType: RingType = RingPair(l.ringType, r.ringType)
//  val refs = l.refs ++ r.refs //assert non-overlapping
//}
//
//case class Project1[L <: RingExpr[L], R <: RingExpr[R]](p: Pair[L, R]) extends RingExpr[Project1[L, R]] {
//  val ringType: RingType = p.l.ringType
//  val refs = p.l.refs
//}
//
//case class Project2[L <: RingExpr[L], R <: RingExpr[R]](p: Pair[L, R]) extends RingExpr[Project2[L, R]] {
//  val ringType: RingType = p.r.ringType
//  val refs = p.r.refs
//}
////
////case class InfMapping[K <: KeyExpr[K], R <: RingExpr[R]](m: K => R) //collections?
//
//case class Plus[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Plus[L, R]] {
//  //l and r must have same ringType
//  val ringType: RingType = l.ringType
//  val refs = l.refs ++ r.refs
//}
//
//case class Negate[R <: RingExpr[R]](r: R) extends RingExpr[Negate[R]] {
//  val ringType: RingType = r.ringType
//  val refs = r.refs
//}
//
//case class Multiply[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Multiply[L, R]] {
//  val ringType: RingType = l.ringType * r.ringType
//  val refs = l.refs ++ r.refs
//}
//
//case class Dot[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Dot[L, R]] {
//  val ringType: RingType = l.ringType.dot(r.ringType)
//  val refs = l.refs ++ r.refs
//}
//
//case class Sum[R <: RingExpr[R]](r: R) extends RingExpr[Sum[R]] {
//  val ringType: RingType = r.ringType.sum
//  val refs = r.refs
//}