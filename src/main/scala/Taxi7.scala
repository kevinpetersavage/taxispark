import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Taxi7 {
  def main(args: Array[String]): Unit = {
    val start = 1
    val end = args(1).toInt

    val lowerLimitToTN = args(2).toInt
    val splits = args(3).toInt
    val range = Range(start, end).reverse

    val conf = new SparkConf().setAppName("taxi").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val values = sc
      .parallelize(range, splits)

    val result = values
      .map(BigInt(_))
      .flatMap{n => createCountsMap(initQueue(n), lowerLimitToTN, n)}
      .groupBy(_._2).map(group => (group._2.map(_._1).min, group._1))
      .collect()

    result.foreach(println(_))
    println(result.length)
  }

  def initQueue(n: BigInt): mutable.PriorityQueue[LocationAndScore] = {
    val ordering = new Ordering[LocationAndScore] {
      override def compare(x: LocationAndScore, y: LocationAndScore): Int = {
        val scoreComparison = y.score.compare(x.score)
        if (scoreComparison == 0){
          val xValueComparison = y.xValue.compare(x.xValue)
          if (xValueComparison == 0){
            y.yValue.compare(x.yValue)
          } else {
            xValueComparison
          }
        } else {
          scoreComparison
        }
      }
    }
    val queue = mutable.PriorityQueue.newBuilder[LocationAndScore](ordering)
    var j = n
    var k = BigInt(0)
    do {
      queue.enqueue(new LocationAndScore(n + k, n-j))
      k = k+1
      val s = Cubes.cubeRt((k * k * k) + (3 * k * k * n) + (3 * k * n * n) - (n * n * n)) + 1
      j = s + n
    } while (n-j > 0)
    queue.enqueue(new LocationAndScore(n + k, 1))
    queue
  }

  def createCountsMap(queue: mutable.PriorityQueue[LocationAndScore], lowerLimitToTN: Int, n: BigInt) : Map[BigInt, Int] = {
    val top = new LocationAndScore(n+1, n+1)
    var count = 0
    var previous: LocationAndScore = null
    var map = Map[BigInt, Int]()
    while (queue.nonEmpty) {
      val location = queue.dequeue()
      if (location != previous) {
        if (previous != null && location.score == previous.score) {
          count = count + 1
        } else {
          if (count >= lowerLimitToTN) {
            map = map + (previous.score -> count)
          }
          count = 1
        }

        val right = new LocationAndScore(location.xValue+1, location.yValue)
        if (right.score < top.score) {
          queue.enqueue(right)
        }

        val down = new LocationAndScore(location.xValue, location.yValue+1)
        if (down.xValue > down.yValue && down.score < top.score) {
          queue.enqueue(down)
        }
      }
      previous = location
    }
    map
  }

  class LocationAndScore(x:BigInt, y:BigInt){
    val score = (x*x*x)+(y*y*y)
    val xValue = x
    val yValue = y

    override def toString = s"$score from $xValue and $yValue"

    def canEqual(other: Any): Boolean = other.isInstanceOf[LocationAndScore]

    override def equals(other: Any): Boolean = other match {
      case that: LocationAndScore =>
        (that canEqual this) &&
          score == that.score &&
          xValue == that.xValue &&
          yValue == that.yValue
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(score, xValue, yValue)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

}
