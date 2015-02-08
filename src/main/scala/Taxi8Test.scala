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
    val inputPairs = (for (k <- kRange) yield Taxi8.producePairsFor(9, k)).flatten

    val values = inputPairs.map(Taxi8.performSumOfCubes)
    assert(values.contains(BigInt(9*9*9*2)))
    assert(values.count(b => BigInt(1729) == b) == 2)

    // should be able to get this too
    //    assert(values.max < BigInt(10*10*10*2))
    //    assert(values.min == BigInt(9*9*9*2))
  }
}