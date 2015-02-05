import org.apache.spark.{SparkConf, SparkContext}

object Taxi8 {
  def main(args: Array[String]): Unit = {
    val start = 1
    val end = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val by = 1000
    val range = start until (end+1) by by

    val conf = new SparkConf().setAppName("taxi").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val results = range.flatMap{ n =>
      val maxK = Math.floor((n+by).toDouble*(Math.pow(2.0, 1.0/3.0)-1.0)).toInt + 1
      val kRange = sc.parallelize(0 until (maxK+1), splits)

      kRange.map{k =>
        (k, 0, n + k)
      }.flatMap{ case (k, minJ, maxJ) =>
        (minJ until (maxJ)).map(j => (n + k,n - j))
      }.filter (location => location._2>0)
        .map{ location =>
        val x = BigInt(location._1)
        val y = BigInt(location._2)
        (x*x*x)+(y*y*y)
      }.groupBy(identity)
        .map(group => (group._2.size, group._1))
        .filter(pair => pair._1 >= lowerLimitToTN)
        .collect()
    }.groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2).min))

    results.foreach(println(_))
  }


  def calculateS(n: BigInt, k: BigInt): Int = {
    Cubes.cubeRt((k * k * k) + (k * k * n * 3) + (k * n * n * 3) - (n * n * n)).toInt
  }
}
