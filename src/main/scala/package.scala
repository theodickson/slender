import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

trait types {
  type Namespace = Map[String,Any]
  type DStreamTest = ((SparkSession,StreamingContext) => Unit)
}

package object slender extends types with Serializable {

  object dsl extends Syntax

}

