import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Taxi8 {

  def main(args: Array[String]): Unit = {
    val lowerLimitToTN = args(0).toInt
    val splits = args(1).toInt
    val bucket = args(2)

    val conf = new SparkConf().setAppName("taxi").setMaster("local[4]")
    val sc = new SparkContext(conf)

    Stream.from(1).filter(fileDoesntExist(bucket)).foreach{ n =>
      val interval = 1
      val maxK = calculateMaxK(n, interval)
      val kRange = sc.parallelize((0 until maxK).reverse, splits)

      kRange.flatMap(producePairsFor(n, _, interval))
          .map(performSumOfCubes)
          .groupBy(identity)
          .map(group => (group._2.size, group._1))
          .filter(pair => pair._1 >= lowerLimitToTN)
          .saveAsTextFile(pathFrom(bucket, n))
    }
  }

  def pathFrom(bucket: String, n: Int) = s"$bucket/$n/"

  def fileDoesntExist(bucket: String)(n :Int) : Boolean = {
    val hdfs = FileSystem.get(new URI(bucket), new Configuration())
    !hdfs.exists(new Path(pathFrom(bucket, n)))
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
      Math.min(Cubes.cubeRt(s).toInt, (n+k).toInt)
    }
  }
}
