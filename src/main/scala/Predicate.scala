//package slender
//
//case class Predicate(p: KeyExpr => IntExpr, keyType: KeyType) extends Function1[KeyExpr,IntExpr] {
//  def apply(k: KeyExpr): IntExpr =
//    if (k.keyType == keyType) p(k)
//    else throw InvalidKeyTypeException(
//      s"Cannot apply predicate requiring key type $keyType to key expression with type ${k.keyType}"
//    )
//}
//
//case class InvalidKeyTypeException(msg: String) extends Exception(msg)