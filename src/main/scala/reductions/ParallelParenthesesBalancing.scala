package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {

    @tailrec
    def loop(idx: Int, acc: Int): Int = {
      if (acc < 0 || idx == chars.length) acc
      else chars(idx) match {
        case '(' => loop(idx + 1, acc + 1)
        case ')' => loop(idx + 1, acc - 1)
        case _ => loop(idx + 1, acc)
      }
    }

    loop(0, 0) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def traverse(idx: Int, until: Int, openSide: Int, closingSide: Int): (Int, Int) = {
      if (idx >= until)
        (openSide, closingSide)
      else chars(idx) match {
        case ')' if openSide == 0   => traverse(idx + 1, until, openSide, closingSide + 1)
        case ')'                    => traverse(idx + 1, until, openSide - 1, closingSide)
        case '('                    => traverse(idx + 1, until, openSide + 1, closingSide)
        case _                      => traverse(idx + 1, until, openSide, closingSide)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {

      val length = until - from

      if (length <= 0 || length <= threshold)
        traverse(from, until, 0, 0)
      else {
        val middle = from + length / 2
        val ((openL, closingL), (openR, closingR)) = parallel(reduce(from, middle), reduce(middle, until))

        val middleSection = openL - closingR
        if (middleSection < 0) {
          (openR, closingL - middleSection) // middle is closing, so increase closing by this amount
        } else {
          (openR + middleSection, closingL) // middle is opening, so increase opening
        }
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
