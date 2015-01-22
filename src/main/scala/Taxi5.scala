import org.apache.spark.{SparkConf, SparkContext}

import scala.math._
import scala.util.Random

object Taxi5 {
  def main(args: Array[String]): Unit = {
    val start = args(0).toInt
    val end = args(1).toInt

    val lowerLimitToTN = args(2).toInt
    val splits = args(3).toInt
    val range = Random.shuffle(Range(start, end).toList)

    val conf = new SparkConf().setAppName("taxi").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val values = sc
      .parallelize(range, splits)

    val result = values.cartesian(values)
      .filter(pair => pair._1 <= pair._2)
      .map(pair => Math.pow(pair._1, 3) + Math.pow(pair._2, 3))
      .groupBy(identity)
      .map(pair => (pair._1, pair._2.size)).filter(_._2>lowerLimitToTN).map(_._1)

    val collect: Array[Double] = result.collect()
    collect.foreach(println(_))
    println(s"${collect.size} of them")
  }

}
