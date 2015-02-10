import com.google.common.base.Stopwatch
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.sqrt

object Taxi8 {

  def main(args: Array[String]): Unit = {
    val stopwatch = new Stopwatch().start()

    val start = 1
    val end = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val m = ((1.0/2.0)*(sqrt(8 * end + 1) +1)).toInt
    val nValues = (start until m+1).reverse.scanLeft(1){_+_}
    val nValuesWithIntervals = nValues.zip(nValues.drop(1)).map(withNext => (withNext._1, withNext._2-withNext._1))

    val conf = new SparkConf().setAppName("taxi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val results = nValuesWithIntervals.flatMap{ nWithInterval =>
      val n = nWithInterval._1
      val interval = nWithInterval._2
      val maxK = calculateMaxK(n, interval)
      val kRange = sc.parallelize(0 until maxK, splits)

      kRange.flatMap(producePairsFor(n, _, interval))
        .map(pair => if (pair._1 > pair._2) pair.swap else pair) // don't understand why we need this
        .distinct()
        .map(performSumOfCubes)
        .groupBy(identity)
        .map(group => (group._2.size, group._1))
        .filter(pair => pair._1 >= lowerLimitToTN)
        .collect()
    }.groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2).min))

    results.foreach(println(_))

    println("time: " + stopwatch)
  }

  def producePairsFor(n: Int, k: Int, interval: Int): TraversableOnce[(Int,Int)] = {
    val start = calculateJLowerBound(BigInt(n), BigInt(k)).getOrElse(0)
    val end = calculateJUpperBound(BigInt(n), BigInt(k), BigInt(interval))
    for (y <- start until end+1) yield (n+k, y)
  }

  def performSumOfCubes: ((Int, Int)) => BigInt = {
    location =>
      val x = BigInt(location._1)
      val y = BigInt(location._2)
      (x * x * x) + (y * y * y)
  }

  def calculateMaxK(n: Int, interval: Int): Int = {
    Math.floor((n + interval).toDouble * (Math.pow(2.0, 1.0 / 3.0) - 1.0)).toInt + 1 + interval
  }

  def calculateJLowerBound(n: BigInt, k: BigInt): Option[Int] = {
    val s = calculateS(n, k)
    if (s < 0){
      None
    } else {
      Some(Cubes.cubeRt(s).toInt)
    }
  }

  def calculateS(n: BigInt, k: BigInt): BigInt = {
    (n * n * n) - (k * k * k) - (k * k * n * 3) - (k * n * n * 3)
  }

  def calculateJUpperBound(n: BigInt, k: BigInt, interval: BigInt): Int = {
    val s = calculateS(n, k) + (n*n*interval*6) + (n*interval*interval*6) + (interval*interval*interval*2)
    if (s < 0){
      throw new UnsupportedOperationException("implies k limit is wrong")
    } else {
      Cubes.cubeRt(s).toInt
    }
  }
}
