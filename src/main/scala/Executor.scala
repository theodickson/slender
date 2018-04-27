package slender

import org.apache.spark.sql.{DataFrame, Dataset}

//trait Planner[T[_]] {
//  def plan[R <: RingExpr[R]](expr: R)(physicalCollections: PhysicalCollectionSet[T]): T[_]
//}
//
//object ListPlanner extends Planner[List] {
//
//  def plan[R <: RingExpr[R]]
//
//  }
//}
////
//e
//
//trait Executor[T] {
//  def execute(expr: RingExpr, physicalCollections: PhysicalCollection[T]*): T
//}
//
//object DataFrameExecutor extends Executor[DataFrame] {
//
//  def execute(expr: RingExpr,
//              collections: PhysicalCollection[DataFrame]*): DataFrame = {
//    val collectionsMap = collections.map(p => (p.ref,p)).toMap
//    expr.assertPhysicalCollectionsValid(collections : _ *)
//    expr match {
//      case SimpleCollection(_, ref) => collectionsMap(ref).collection
//      case Plus(l, r) => execute(l, collections: _ *).union(execute(r, collections: _ *))
//      case _ => ???
//    }
//  }
//}
//