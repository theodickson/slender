package slender

//courtesy of http://missingfaktor.blogspot.co.uk/2013/12/optional-implicit-trick-in-scala.html
case class Perhaps[T](value: Option[T])

object Perhaps {
  implicit def perhaps[T](implicit ev: T = null): Perhaps[T] = Perhaps(Option(ev))
}
