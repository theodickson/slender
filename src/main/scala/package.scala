trait types {
  type BoundVars = Map[String,Any]
}

package object slender extends types with Serializable {

  object dsl extends Syntax with Variables

}


