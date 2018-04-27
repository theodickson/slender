//package slender
//
//import org.apache.spark.sql.{Column, DataFrame, Dataset}
//import org.apache.spark.sql.types.{DataType, StructType, IntegerType => SparkIntegerType, StringType => SparkStringType}
//import org.apache.spark.sql.functions.{col, lit}
//
//trait PhysicalCollection[T] {
//  def ref: String
//  def collection: T
//  def keyType: KeyType
//  def ringType: RingType
//  def collectionType: (KeyType,RingType) = (keyType,ringType)
//}
//
//case class DataFrameKey(cols: Seq[Column])
//
//object DataFrameKey {
//  def apply = DataFrameKey(Seq())
//  def apply(colNames: String*) = DataFrameKey(colNames.map(col))
//}
//
//case class DataFrameValue(cols: Seq[Column])
//
//object DataFrameValue {
//  def apply = DataFrameValue(Seq(lit(1)))
//  def apply(colNames: String*) = DataFrameKey(colNames.map(col))
//}
//
//trait DataFrameCollection extends PhysicalCollection[DataFrame] {
//  def keyRef: Array[String]
//  def valueRef: Array[String]
//}
//
//case class DataFrameCollection(collection: DataFrame, ref: String) extends PhysicalCollection[DataFrame] {
//  val keyType = utils.keyTypeFromSparkSchema(collection.schema)
//  val ringType = IntType
//}
//
//object utils {
//
//  def keyTypeFromSparkDataType(dataType: DataType): KeyType = dataType match {
//    case SparkIntegerType => DomIntType
//    case SparkStringType => DomStringType
//    case nested : StructType => keyTypeFromSparkSchema(nested)
//  }
//  def keyTypeFromSparkSchema(schema: StructType): KeyType = schema.size match {
//    case 0 => UnitType
//    case 1 => keyTypeFromSparkDataType(schema.head.dataType)
//    case 2 => KeyPair(keyTypeFromSparkDataType(schema(0).dataType), keyTypeFromSparkDataType(schema(1).dataType))
//  }
//}
////case class SimplePhysicalCollection[T](ref: String, collection: T) extends PhysicalCollection[T]
//
////class PhysicalCollectionSet[O[_]](val physCols: PhysicalCollection[O, _]*) {
////  def get(ref: String): Option[PhysicalCollection[O, _]] = physCols.filter(_.ref == ref).headOption
////  def apply(ref: String): PhysicalCollection[O, _] = physCols.filter(_.ref == ref).head
////}
