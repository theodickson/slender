//package slender
//
//import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
//
//import scala.collection.GenTraversableOnce
//
//object implicits {
//
//  implicit class DataFrameImplicits(df: DataFrame) {
//    def <=>[T](data: GenTraversableOnce[T])(implicit enc: Encoder[T]): Boolean =
//      df.as[T].collect.toList == data.toList
//
//    def <~>[T](data: TraversableOnce[T])(implicit enc: Encoder[T], ord: Ordering[T]): Boolean =
//      df.as[T].collect.toList.sorted == data.toList.sorted
//  }
//
//}

object dsl {




  //  implicit def physCollToCollSet[O[_], I](physColl: PhysicalCollection[O, I]): PhysicalCollectionSet[O] =
  //    new PhysicalCollectionSet[O](physColl)
  //
  //  implicit def physCollToCollSet[O[_]](physColls: Seq[PhysicalCollection[O, _]]): PhysicalCollectionSet[O] =
  //    new PhysicalCollectionSet[O](physColls: _*)
}
