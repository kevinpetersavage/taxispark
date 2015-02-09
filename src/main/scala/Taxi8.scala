import com.google.common.base.Stopwatch
import org.apache.spark.{SparkConf, SparkContext}

object Taxi8 {

  def main(args: Array[String]): Unit = {
    val stopwatch = new Stopwatch().start()

    val start = 1
    val end = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val interval = 1
    val range = start until (end+1) by interval

    val conf = new SparkConf().setAppName("taxi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val results = range.flatMap{ n =>
      val maxK = calculateMaxK(n, interval)
      val kRange = sc.parallelize(0 until maxK, splits)

      kRange.flatMap(producePairsFor(n, _))
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

  def producePairsFor(n: Int, k: Int): TraversableOnce[(Int,Int)] = {
    val start = calculateJBound(BigInt(n), BigInt(k)).getOrElse(0)
    val end = calculateJBound(BigInt(n+1), BigInt(k-1)).getOrElse(n)
    for (y <- start until end+1) yield (n+k, y)
  }

  def performSumOfCubes: ((Int, Int)) => BigInt = {
    location =>
      val x = BigInt(location._1)
      val y = BigInt(location._2)
      (x * x * x) + (y * y * y)
  }

  def calculateMaxK(n: Int, interval: Int): Int = {
    Math.floor((n + interval).toDouble * (Math.pow(2.0, 1.0 / 3.0) - 1.0)).toInt + 2
  }

  def calculateJBound(n: BigInt, k: BigInt): Option[Int] = {
    val value = (n * n * n) - (k * k * k) - (k * k * n * 3) - (k * n * n * 3)
    if (value < 0){
      None
    } else {
      Some(Cubes.cubeRt(value).toInt)
    }
  }
}
