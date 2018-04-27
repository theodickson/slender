//package slender
//import scala.reflect.Manifest
//
////Alternative is
//sealed trait RingExpr[R <: RingExpr[R]] {
//  def ringType: RingType
//
//  def collections: Map[String, Manifest[_]]
//
//  def assertPhysicalCollectionsValid[O[_]](physicalCollections: PhysicalCollectionSet[O]): Unit = {
//    //val collectionsMap = physicalCollections.map(p => (p.ref,p)).toMap //check distinct?
////    def getPhysicalCollection(ref: String): Option[PhysicalCollection[O, _]] =
////      physicalCollections.filter(_.ref == ref).headOption
//    //First check all needed refs appear in the given physical collections, and that the inner types of the
//    //respective inner collections match those needed by the ref in the RingExpr.
//    collections foreach { case (ref, tag) =>
//      physicalCollections.get(ref) match {
//        case None => throw new IllegalArgumentException(
//          s"Ref $ref does not appear in the given physical collections"
//        )
//        case Some(physColl) => if (physColl.innerTag != tag)
//          throw new IllegalArgumentException(
//            s"Ref $ref has different tags in the physical collection and the ring expression."
//          )
//      }
//    }
////    //Then check that all outer types (E.g. RDD, Dataset, List, etc) of the physical collections are the same
////    //Possibly unnecessary
////    val headOuterTag = physicalCollections.head.outerTag
////    physicalCollections foreach { p =>
////      if (p.outerTag != headOuterTag)
////        throw new IllegalArgumentException("Not all physical collections have the same type.")
////    }
//  }
//}
//
//case class Collection[K](ref: String)(implicit tag: Manifest[K]) extends RingExpr[Collection[K]] {
//  val ringType: RingType = MappingType(Dom[K](), IntType)
//  val collections = Map(ref -> tag)
//}
//
//case class Pair[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Pair[L, R]] {
//  val ringType: RingType = RingPair(l.ringType, r.ringType)
//  val collections = l.collections ++ r.collections //assert non-overlapping
//}
//
//case class Project1[L <: RingExpr[L], R <: RingExpr[R]](p: Pair[L, R]) extends RingExpr[Project1[L, R]] {
//  val ringType: RingType = p.l.ringType
//  val collections = p.l.collections
//}
//
//case class Project2[L <: RingExpr[L], R <: RingExpr[R]](p: Pair[L, R]) extends RingExpr[Project2[L, R]] {
//  val ringType: RingType = p.r.ringType
//  val collections = p.r.collections
//}
//
//case class InfMapping[K <: KeyExpr[K], R <: RingExpr[R]](m: K => R) //collections?
//
//case class Plus[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Plus[L, R]] {
//  //l and r must have same ringType
//  val ringType: RingType = l.ringType
//  val collections = l.collections ++ r.collections
//}
//
//case class Negate[R <: RingExpr[R]](r: R) extends RingExpr[Negate[R]] {
//  val ringType: RingType = r.ringType
//  val collections = r.collections
//}
//
//case class Multiply[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Multiply[L, R]] {
//  val ringType: RingType = l.ringType * r.ringType
//  val collections = l.collections ++ r.collections
//}
//
//case class Dot[L <: RingExpr[L], R <: RingExpr[R]](l: L, r: R) extends RingExpr[Dot[L, R]] {
//  val ringType: RingType = l.ringType.dot(r.ringType)
//  val collections = l.collections ++ r.collections
//}
//
//case class Sum[R <: RingExpr[R]](r: R) extends RingExpr[Sum[R]] {
//  val ringType: RingType = r.ringType.sum
//  val collections = r.collections
//}