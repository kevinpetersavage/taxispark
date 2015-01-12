
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.util.Random

object Taxi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val start = 1
    val end = args(0).toInt
    val lowerLimit = args(1).toInt
    val lowerBounds = BigInt(args(2))
    val upperBounds = BigInt(args(3))
    val splits = args(4).toInt
    val range = Random.shuffle[Int, IndexedSeq](Range(start, end))

    val cubes = sc
      .parallelize(range, splits)
      .map(BigInt(_))
      .map(x => x*x*x)

    val results = cubes
      .cartesian(cubes)
      .filter(pair => pair._1 <= pair._2)
      .map(pair => pair._1 + pair._2)
      .filter(_ < upperBounds)
      .filter(_ > lowerBounds)
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .filter(counted => counted._2 > lowerLimit)
      .groupBy(counted => counted._2)
      .map(byCount => (byCount._1, byCount._2.map(_._1).min))

    results.collect().foreach(println)
  }
}
