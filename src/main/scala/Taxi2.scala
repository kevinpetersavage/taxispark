
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Taxi2 {
  def main(args: Array[String]): Unit = {
    val lowestNumberToCheck = BigInt(args(0))
    val highestNumberToCheck = BigInt(args(1))

    val start = Cubes.cubeRt(lowestNumberToCheck/2).toInt
    val end = Cubes.cubeRt(highestNumberToCheck-1).toInt

    val lowerLimitToTN = args(2).toInt
    val splits = args(3).toInt
    val range = Random.shuffle(Range(start, end).toList)



    val conf = new SparkConf().setAppName("taxi").setMaster("local[1]")
    val sc = new SparkContext(conf)

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
        .filter(_._2 > lowerLimitToTN)
    }.distinct().sortBy(_._1).take(1)

    results.foreach(println)
  }


}
