//package slender
//
//sealed trait PhysicalCollection[O[_], I] {
//  def outerTag: Manifest[O[_]]
//  def innerTag: Manifest[I]
//
//  def ref: String
//  def collection: O[I]
//
//}
//
//case class SimplePhysicalCollection[O[_], I](ref: String, collection: O[I])
//                                            (implicit val outerTag: Manifest[O[_]], val innerTag: Manifest[I])
//  extends PhysicalCollection[O, I]
//
//class PhysicalCollectionSet[O[_]](val physCols: PhysicalCollection[O, _]*) {
//  def get(ref: String): Option[PhysicalCollection[O, _]] = physCols.filter(_.ref == ref).headOption
//  def apply(ref: String): PhysicalCollection[O, _] = physCols.filter(_.ref == ref).head
//}
