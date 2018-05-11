package slender.execution

import slender._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe.definitions._


object implicits {

  val MapTpe = typeOf[scala.collection.immutable.Map[_,_]].typeConstructor
  val Tuple2Tpe = typeOf[(_,_)].typeConstructor
  val Tuple3Tpe = typeOf[(_,_,_)].typeConstructor
  val StringTpe = typeOf[java.lang.String]

  //use a macro instead to get rid of the need to use a ref here
  implicit def toPhysicalCollection[T : TypeTag](p: (T, String)): PhysicalCollection = {
    val mappingType = getMappingType(p._1)
    PhysicalCollection(mappingType.k, mappingType.r, p._2)
  }

  def getMappingType[T : TypeTag](t: T): MappingType = toMappingType(typeOf[T])

  def toMappingType(t: Type): MappingType = toRingType(t) match {
    case m@MappingType(_,_) => m
    case _ => throw new IllegalStateException()
  }

  def toKeyType(t: Type): ResolvedKeyType = (t.typeConstructor,t.typeArgs) match {
    case (Tuple2Tpe,List(tpe1,tpe2)) => KeyPairType(toKeyType(tpe1), toKeyType(tpe2))
    case (Tuple3Tpe,List(tpe1,tpe2,tpe3)) => KeyTuple3Type(toKeyType(tpe1), toKeyType(tpe2), toKeyType(tpe3))
    case (primitive,List()) => DomKeyType(primitive)
    case _ => throw new IllegalStateException(s"Invalid type $t for conversion to KeyType.")
  }

  def toRingType(t: Type): ResolvedRingType = (t.typeConstructor,t.typeArgs) match {
    case (IntTpe,_) => IntType
    case (MapTpe,List(kTpe,vTpe)) => MappingType(toKeyType(kTpe),toRingType(vTpe))
    case (Tuple2Tpe,List(tpe1,tpe2)) => RingPairType(toRingType(tpe1), toRingType(tpe2))
    case (Tuple3Tpe,List(tpe1,tpe2,tpe3)) => RingTuple3Type(toRingType(tpe1), toRingType(tpe2), toRingType(tpe3))
    case _ => throw new IllegalStateException(s"Invalid type $t for conversion to RingType.")
  }

}