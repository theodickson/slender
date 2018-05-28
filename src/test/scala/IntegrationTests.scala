package slender

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import scala.collection.immutable.Map

class IntegrationTests extends FunSuite with TestUtils {

  import implicits._

  val spark = SparkSession.builder()
    .appName("test")
    .config("spark.master", "local")
    .getOrCreate()

  implicit val sc = spark.sparkContext


  test("Packet query") {
    val serverData = Map(
      (1,
        Map(
          (1,"1") -> 1,
          (1,"2") -> 1,
          (3,"0") -> 1
        ),
        Map(
          (1,"1") -> 1,
          (1,"3") -> 1,
          (2,"1") -> 1,
          (3,"0") -> 1
        )
      ) -> 1
    )

    val server = PhysicalCollection(serverData)

    val serverRdd = PhysicalCollection(PairRDD(sc.parallelize(serverData.toList)))

    val bagOfInts = PhysicalCollection(Map(1 -> 1))
//    val inner = For ((Z1,Z2) <-- (toRing(Y1) dot toRing(Y2))) Yield ((Z1,Z2))

//    println(inner)
//    val query = For ((X,Y1,Y2) <-- server) Yield (
//      (X,
//       toK(For (Z <-- (toRing(Y1) dot toRing(Y2))) Yield Z)
//      )
//    )

    //val query = For (X <-- server) Yield ()

    val query = For ((X,Y1,Y2) <-- serverRdd) Yield ((X,toK(
      For (((Z1,W1),(Z2,W2)) <-- (toRing(Y1) dot toRing(Y2)) iff (W1 === W2)) Yield ((Z1,W1),(Z2,W2)))
    ))

    val query2 = For (X <-- bagOfInts iff (X === IntKeyExpr(0))) Yield X

//    val inner = For (Z <-- (toRing(Y1) dot toRing(Y2))) Yield Z
////
//    implicit val make = tuple2MakeKeyExpr[_X,_X,_X,_X]

//    val query = For (Tuple3VariableExpr(X,Y1,Y2) <-- server) Yield ((X,X))

//    val query = For (X <-- bagOfInts) Collect 1
//    val query = For (X <-- bagOfInts) Collect ((1,1))

//    val nonDslQuery = SumExpr(
//      MultiplyExpr(
//        bagOfInts,
//        InfiniteMappingExpr(X,SngExpr(Tuple2VariableExpr(X,X),IntExpr(1)))
//      )
//    )
//
//    println(nonDslQuery.resolve.eval)
//    query.resolve
//    println(query.isResolved)
//    println(query2.isResolved)
//
    println(query.eval)
//    printType(query.c1)


  }
}
