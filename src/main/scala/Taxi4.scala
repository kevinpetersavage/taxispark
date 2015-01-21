import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
import scala.math._

object Taxi4 {
  def main(args: Array[String]): Unit = {
    val start = args(0).toInt
    val end = args(1).toInt

    val lowerLimitToTN = args(2).toInt
    val splits = args(3).toInt
    val range = Random.shuffle(Range(start, end).toList)

    val conf = new SparkConf().setAppName("taxi").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val values = sc
      .parallelize(range, splits)

    val precision = pow(10, 7)

    val result = values.cartesian(values)
      .filter(pair => pair._1 <= pair._2)
      .map(pair => (3*log(pair._1)) + log1p(exp(3 * (log(pair._2) - log(pair._1)))))
      .groupBy(v => floor(v*precision)/precision)
      .map(pair => (pair._1, pair._2.size)).filter(_._2>lowerLimitToTN)
      .map(p=> exp(p._1))


    val collect: Array[Double] = result.collect()
    collect.foreach(println(_))
    println(s"${collect.size} of them")
  }

}
