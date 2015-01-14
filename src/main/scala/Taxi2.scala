
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Taxi2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val start = 1
    val end = args(0).toInt
    val lowerLimit = args(1).toInt
    val splits = args(2).toInt
    val range = Random.shuffle(Range(start, end).toList)

    val values = sc
      .parallelize(range, splits)

    val results = values.flatMap { x =>
      val xToPow3 = BigInt(x).pow(3)
      val columns = (1 until x).reverse.takeWhile(y => BigInt(y).pow(3) * 2 > xToPow3)

      columns.flatMap(y => {
          val yToPow3 = BigInt(y).pow(3)
          (1 until y)
            .map(y => yToPow3 + BigInt(y).pow(3))
        })
        .groupBy(identity)
        .map(t => (t._1, t._2.length))
        .filter(_._2 > lowerLimit)
    }.distinct().sortBy(_._1).take(1)

    results.foreach(println)
  }
}
