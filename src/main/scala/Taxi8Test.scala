import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.immutable.IndexedSeq

class Taxi8Test extends FunSuite with BeforeAndAfterEach with ShouldMatchers {
  test("limit for maxk"){
    assert(Taxi8.calculateMaxK(9, 1) == 3)
  }

  test("content of sums"){
    val n = 9
    val kRange = Range(0, Taxi8.calculateMaxK(n, 1))
    val inputPairs = (for (k <- kRange) yield Taxi8.producePairsFor(n, k)).flatten

    val values = inputPairs.map(Taxi8.performSumOfCubes).sorted
    assert(values.count(b => BigInt(1729) == b) == 2)

    // should be able to get this too
    val expected = (for (x <- 1 until 15; y <- 1 until 15) yield (x * x * x) + (y * y * y))
      .filter(_ < 10 * 10 * 10 * 2)
      .filter(_ > 9 * 9 * 9 * 2)
    val diff = values.diff(expected)
    assert(diff.size==6) // 3 overrun and 3 underrun
  }


  test("content of sums for tn 3"){
    val n = 352
    val kRange = Range(0, Taxi8.calculateMaxK(n, 1))
    val inputPairs = (for (k <- kRange) yield Taxi8.producePairsFor(n, k)).flatten

    val values = inputPairs.map(Taxi8.performSumOfCubes).sorted
    val counts: Map[BigInt, Int] = values.groupBy(identity).map(group => (group._1, group._2.size)).filter(_._2 == 3)
    assert(counts == Map(BigInt(87539319)->3))
  }
}