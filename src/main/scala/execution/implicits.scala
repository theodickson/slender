package slender.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import slender._

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import scala.reflect.runtime.universe.{Tree, Type, TypeTag, typeOf}
import scala.reflect.runtime.universe.definitions._


object implicits {

  val MapTpe = typeOf[Map[_,_]].typeConstructor
  val ListTpe = typeOf[List[_]].typeConstructor
  val RddTpe = typeOf[RDD[_]].typeConstructor
  val DatasetTpe = typeOf[Dataset[_]].typeConstructor
  val Tuple2Tpe = typeOf[(_,_)].typeConstructor
  val Tuple3Tpe = typeOf[(_,_,_)].typeConstructor
  val StringTpe = typeOf[java.lang.String]

  //todo - this doesnt work from the REPL
  implicit def mapToExpr[K,V](m: Map[K,V])(implicit ev: TypeTag[Map[K,V]]): PhysicalCollection =
    macro mapToExprImpl[K,V]

  implicit def datasetToExpr[K,V](ds: Dataset[(K,V)])(implicit ev: TypeTag[Dataset[(K,V)]]): PhysicalCollection =
    macro datasetToExprImpl[K,V]

  implicit def rddToExpr[K,V](ds: RDD[(K,V)])(implicit ev: TypeTag[RDD[(K,V)]]): PhysicalCollection =
  macro rddToExprImpl[K,V]

  def mapToExprImpl[K,V](c: Context)(m: c.Expr[Map[K,V]])
                        (ev: c.Expr[TypeTag[Map[K,V]]]): c.Expr[PhysicalCollection] = {
    import c.universe._
    val refRep = show(m.tree).split('.').last //todo - why am i getting rid of the tree then rebuilding later?
    c.Expr[PhysicalCollection](
      q"""
        val mappingType = getMappingType($m)($ev)
        PhysicalCollection(mappingType.k, mappingType.r, $refRep, dist=false)
        """
    )
  }

  def datasetToExprImpl[K,V](c: Context)(ds: c.Expr[Dataset[(K,V)]])
                        (ev: c.Expr[TypeTag[Dataset[(K,V)]]]): c.Expr[PhysicalCollection] = {
    import c.universe._
    val refRep = show(ds.tree).split('.').last //todo - why am i getting rid of the tree then rebuilding later?
    c.Expr[PhysicalCollection](
      q"""
        val mappingType = getMappingType($ds)($ev)
        PhysicalCollection(mappingType.k, mappingType.r, $refRep, dist=true)
        """
    )
  }

  def rddToExprImpl[K,V](c: Context)(ds: c.Expr[RDD[(K,V)]])
                        (ev: c.Expr[TypeTag[RDD[(K,V)]]]): c.Expr[PhysicalCollection] = {
    import c.universe._
    val refRep = show(ds.tree).split('.').last //todo - why am i getting rid of the tree then rebuilding later?
    c.Expr[PhysicalCollection](
      q"""
        val mappingType = getMappingType($ds)($ev)
        PhysicalCollection(mappingType.k, mappingType.r, $refRep, dist=true)
        """
    )
  }

  def getMappingType[T : TypeTag](t: T): FiniteMappingType = toRingType(typeOf[T]).asInstanceOf[FiniteMappingType]

  def toKeyType(t: Type): KeyType = (t.typeConstructor,t.typeArgs) match {
    case (Tuple2Tpe,tpes) => ProductKeyType(tpes.map(toKeyType) : _ *)
    case (Tuple3Tpe,tpes) => ProductKeyType(tpes.map(toKeyType) : _ *)
    case (primitive,List()) => PrimitiveKeyType(primitive)
    case _ => BoxedRingType(toRingType(t))
    //case _ => throw new IllegalStateException(s"Invalid type $t for conversion to KeyType.")
  }

  def toRingType(t: Type): RingType = (t.typeConstructor,t.typeArgs) match {
    case (IntTpe,_) => IntType
    case (tpe,List(kTpe,vTpe)) if tpe <:< MapTpe => {
      FiniteMappingType(toKeyType(kTpe), toRingType(vTpe))
    }
//    case (ListTpe,List(tpe)) if (tpe.typeConstructor == Tuple2Tpe) => {
//      val kTpe = tpe.typeArgs(0)
//      val vTpe = tpe.typeArgs(1)
//      FiniteMappingType(toKeyType(kTpe),toRingType(vTpe),dist=true)
//    }
    case (distTpe,List(tpe)) if tpe.typeConstructor == Tuple2Tpe && (distTpe == RddTpe || distTpe == DatasetTpe) => {
      val kTpe = tpe.typeArgs(0)
      val vTpe = tpe.typeArgs(1)
      FiniteMappingType(toKeyType(kTpe),toRingType(vTpe),dist=true)
    }
    case (Tuple2Tpe,tpes) => ProductRingType(tpes.map(toRingType) : _ *)
    case (Tuple3Tpe,tpes) => ProductRingType(tpes.map(toRingType) : _ *)
    case _ => throw new IllegalStateException(s"Invalid type $t for conversion to RingType.")
  }


  def sparkTestMacro: Any = macro sparkTestMacroImpl

  def sparkTestMacroImpl(c: Context): c.Expr[Any] = {
    import c.universe._
    c.Expr[Any](
      q"""
      val spark = org.apache.spark.sql.SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
      import spark.implicits._
      val stringCounts = Map(
        ("a",1),
        ("b",2),
        ("c",3)
      ).toList.toDS
      stringCounts.map(x => x._2)
      """
    )
  }

//  def evalExpr(expr: Expr[_], codeGen: CodeGen, ctx: ExecutionContext): Any = macro evalExprImpl
//
//  def evalExprImpl(c: Context)(codeGen: c.Expr[CodeGen], ctx: c.Expr[ExecutionContext]): c.Expr[Any] = {
//    import c.universe._
//    c.Expr[Any](
//      q
//    )
//  }

}