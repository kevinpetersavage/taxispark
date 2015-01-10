
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Taxi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi")
    val sc = new SparkContext(conf)

    val start = 1
    val end = args(0).toInt
    val lowerLmit = args(1).toInt
    val range = Range(start, end)

    val cubes = sc
      .parallelize(range, 50)
      .map(BigInt(_))
      .map(x => x*x*x)
      .cache()

    val results = cubes
      .cartesian(cubes)
      .filter(pair => pair._1 <= pair._2)
      .map(pair => pair._1 + pair._2)
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .filter(counted => counted._2 > lowerLmit)
      .groupBy(counted => counted._2)
      .map(byCount => (byCount._1, byCount._2.map(_._1).min))

    results.collect().foreach(println)
  }
}
