import org.apache.spark.{SparkConf, SparkContext}

object Taxi8 {

  def main(args: Array[String]): Unit = {
    val start = 1
    val end = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val interval = 1000
    val range = start until (end+1) by interval

    val conf = new SparkConf().setAppName("taxi").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val results = range.flatMap{ n =>
      val maxK = calculateMaxK(n, interval)
      val kRange = sc.parallelize(0 until (maxK+1), splits)
      val lowerLimit = n*n*n*2
      val upperLimit = (n+interval)*(n+interval)*(n+interval)*2

      kRange.flatMap(producePairsFor(n, _))
        .filter (location => location._2>0)
        .map(performSumOfCubes)
        .filter(sum => sum >= lowerLimit)
        .filter(sum => sum < upperLimit)
        .groupBy(identity)
        .map(group => (group._2.size, group._1))
        .filter(pair => pair._1 >= lowerLimitToTN)
        .collect()
    }.groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2).min))

    results.foreach(println(_))
  }

  def producePairsFor(n: Int, k: Int): TraversableOnce[(Int,Int)] = for (y <- 1 until (n+k+1)) yield (n+k, y)

  def performSumOfCubes: ((Int, Int)) => BigInt = {
    location =>
      val x = BigInt(location._1)
      val y = BigInt(location._2)
      (x * x * x) + (y * y * y)
  }

  def calculateMaxK(n: Int, interval: Int): Int = {
    Math.floor((n + interval).toDouble * (Math.pow(2.0, 1.0 / 3.0) - 1.0)).toInt + 2
  }

  def calculateS(n: BigInt, k: BigInt): Int = {
    Cubes.cubeRt((k * k * k) + (k * k * n * 3) + (k * n * n * 3) - (n * n * n)).toInt
  }
}
