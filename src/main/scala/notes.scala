//val y = Map(
//  ("a",((1,1),(1,1))),
//  ("b",((2,2),(2,2))),
//  ("c",((3,3),(3,3)))
//)
//
//
//((x1: Map[String, Map[String, ((Int,Int),(Int,Int))]]) => x1.values.foldRight(Map.empty[String, ((Int,Int),(Int,Int))])(((x1: Map[String, ((Int,Int),(Int,Int))], x2: Map[String, ((Int,Int),(Int,Int))]) => x1.++(x2.map({
//  case Tuple2((k @ _), (v @ _)) => k.->(((x1: ((Int,Int),(Int,Int)), x2: ((Int,Int),(Int,Int))) => Tuple2(((x1: (Int,Int), x2: (Int,Int)) => Tuple2(((x1: Int, x2: Int) => x1.+(x2))(x1._1, x2._1), ((x1: Int, x2: Int) => x1.+(x2))(x1._2, x2._2)))(x1._1, x2._1), ((x1: (Int,Int), x2: (Int,Int)) => Tuple2(((x1: Int, x2: Int) => x1.+(x2))(x1._1, x2._1), ((x1: Int, x2: Int) => x1.+(x2))(x1._2, x2._2)))(x1._2, x2._2)))(v, x1.getOrElse(k, Tuple2(Tuple2(0, 0), Tuple2(0, 0)))))
//})))))
//
//val inner = y map {
//  case (k,v1) => k ->
//    (((x1: ((Int,Int),(Int,Int)), x2: Map[String, Int]) => x2.mapValues(((v) => ((x1: ((Int,Int),(Int,Int)), x2: Int) => Tuple2(((x1: (Int,Int), x2: Int) => Tuple2(((x1: Int, x2: Int) => x1.*(x2))(x1._1, x2), ((x1: Int, x2: Int) => x1.*(x2))(x1._2, x2)))(x1._1, x2), ((x1: (Int,Int), x2: Int) => Tuple2(((x1: Int, x2: Int) => x1.*(x2))(x1._1, x2), ((x1: Int, x2: Int) => x1.*(x2))(x1._2, x2)))(x1._2, x2)))(v, x1))))(v1, ((x: String) => ((x1: String, x2: Int) => Map(x1.->(x2)))(x, 1))(k)))
//}