//val outer: Map[String, Map[String, Int]] => Map[String,Int] = (x1: Map[String, Map[String, Int]]) =>
//
//  x1.values.foldRight(Map.empty[String, Int])((x1: Map[String, Int], x2: Map[String, Int]) =>
//    x1.++(x2).map {
//      case (k,v) => k.->(((x1: Int, x2: Int) => x1.+(x2))(v, x1.getOrElse(k, 0)))
//    }
//  )
//
//val bag: Map[String,Map[String,Int]] = Map(
//  ("b", Map("b" -> 2)),
//  ("c", Map("c" -> 2)),
//  ("d", Map("d" -> 2))
//)
//
//val inner = bag.map {
//  case (k,v1) => k.->(((x1: scala.collection.immutable.Map[String, Int], x2: Int) => x1.mapValues(((v) => ((x1: Int, x2: Int) => x1.*(x2))(v, x2))))(v1, ((x: String) => 3)(k)))
//}
