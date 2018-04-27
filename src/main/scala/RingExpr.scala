package slender
import scala.reflect.Manifest

//TODO - equality checks with refs

sealed trait RingExpr {
  def ringType: RingType
  def refs: Map[String,(KeyType,RingType)]
  //free var check?
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

case class IntExpr(value: Int) extends RingExpr {
  val ringType = IntType
  val refs = Map.empty[String,(KeyType,RingType)]
}
case class SimpleCollection(keyType: KeyType, ref: String) extends FiniteMappingRingExpr {
  val valueType = IntType
  val refs = Map(ref -> (keyType,ringType))
}

case class Collection(keyType: KeyType, valueType: RingType, ref: String) extends FiniteMappingRingExpr {
  val refs = Map(ref -> (keyType,ringType))
}

case class InfMapping(key: VarKeyExpr, value: RingExpr) extends MappingRingExpr {
  //checks on vars in ring expr?
  def keyType = key.keyType
  val valueType = value.ringType
  val refs = value.refs //Possibly not always true and quite a problem if so
}

case class Pair(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = RingPairType(left.ringType, right.ringType)
}

case class Project1(p: Pair) extends RingExpr {
  val ringType: RingType = p.left.ringType
  val refs = p.left.refs
}

case class Project2(p: Pair) extends RingExpr {
  val ringType: RingType = p.right.ringType
  val refs = p.right.refs
}

case class Plus(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType + right.ringType
}

case class Negate(child: RingExpr) extends UnaryRingExpr

case class Multiply(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType * right.ringType
}

case class Dot(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType dot right.ringType
}

case class Sum(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType.sum
}

case class Predicate(keyType: KeyType)(k: KeyExpr, p: KeyExpr => IntExpr) extends RingExpr {
  assert()
  val ringType: RingType = IntType
  val refs = Map.empty[String,(KeyType,RingType)] //???
}

case class Not(child: RingExpr) extends UnaryRingExpr

case class Sng(k: KeyExpr, r: RingExpr) extends MappingRingExpr {
  val keyType = k.keyType
  val valueType = r.ringType
  val refs = r.refs //???
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