package slender.algebra

import com.twitter.algebird.Ring
import com.twitter.algebird.{Product2Ring,Product3Ring}

object implicits {
}

trait SlenderRing[A] extends Ring[A] {
  def dot[B](l: A, r: B)(implicit ringB: SlenderRing[B]): Any
}

