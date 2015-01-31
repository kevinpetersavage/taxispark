
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.util.Random

object Taxi6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val start = 1
    val end = args(0).toInt
    val lowerLimit = args(1).toInt
    val stepSize = args(2).toInt
    val splits = args(3).toInt
    val summer = args(4)
    val sumCommand = s"${summer} ${stepSize} ${end}"
    val range = Range(start, end, stepSize)

    val values = sc
      .parallelize(range, splits)

    val results = values
      .pipe(sumCommand)
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .filter(counted => counted._2 > lowerLimit)
      .groupBy(counted => counted._2)
      .map(byCount => (byCount._1, byCount._2.map(_._1).map(BigInt(_)).min))

    results.collect().foreach(println)
  }
}
