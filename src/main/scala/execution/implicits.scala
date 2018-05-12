package slender.execution

import slender._

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import scala.reflect.runtime.universe.{TypeTag,typeOf,Type}
import scala.reflect.runtime.universe.definitions._


object implicits {

  val MapTpe = typeOf[scala.collection.immutable.Map[_,_]].typeConstructor
  val Tuple2Tpe = typeOf[(_,_)].typeConstructor
  val Tuple3Tpe = typeOf[(_,_,_)].typeConstructor
  val StringTpe = typeOf[java.lang.String]

  //todo - this doesnt work from the REPL
  implicit def toExpr[T](t: T)(implicit ev: TypeTag[T]): PhysicalCollection = macro toExprImpl[T]

  def toExprImpl[T](c: Context)(t: c.Expr[T])(ev: c.Expr[TypeTag[T]]): c.Expr[PhysicalCollection] = {
     import c.universe._
     val refRep = show(t.tree).split('.').last //todo - why am i getting rid of the tree then rebuilding later?
     c.Expr[PhysicalCollection](
       q"""
        val mappingType = getMappingType($t)($ev)
        PhysicalCollection(mappingType.k, mappingType.r, $refRep)
        """
    )
  }

  def getMappingType[T : TypeTag](t: T): FiniteMappingType = toMappingType(typeOf[T])

  def toMappingType(t: Type): FiniteMappingType = toRingType(t) match {
    case m@FiniteMappingType(_,_) => m
    case _ => throw new IllegalStateException()
  }

  def toKeyType(t: Type): ResolvedKeyType = (t.typeConstructor,t.typeArgs) match {
    case (Tuple2Tpe,tpes) => ProductKeyType(tpes.map(toKeyType) : _ *)
    case (Tuple3Tpe,tpes) => ProductKeyType(tpes.map(toKeyType) : _ *)
    case (primitive,List()) => DomKeyType(primitive)
    case _ => throw new IllegalStateException(s"Invalid type $t for conversion to KeyType.")
  }

  def toRingType(t: Type): ResolvedRingType = (t.typeConstructor,t.typeArgs) match {
    case (IntTpe,_) => IntType
    case (MapTpe,List(kTpe,vTpe)) => FiniteMappingType(toKeyType(kTpe),toRingType(vTpe))
    case (Tuple2Tpe,tpes) => ProductRingType(tpes.map(toRingType) : _ *)
    case (Tuple3Tpe,tpes) => ProductRingType(tpes.map(toRingType) : _ *)
    case _ => throw new IllegalStateException(s"Invalid type $t for conversion to RingType.")
  }

}