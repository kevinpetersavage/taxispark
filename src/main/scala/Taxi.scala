
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Taxi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi").setMaster("local[2]")

    val start = 1
    val end = 20000
    val range = Range(start, end)

    val cubes = new SparkContext(conf)
      .parallelize(range)
      .map(BigInt(_))
      .map(x => x*x*x)
      .cache()

    val results = cubes
      .cartesian(cubes)
      .filter(pair => pair._1 <= pair._2)
      .map(pair => pair._1 + pair._2)
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .filter(counted => counted._2 > 2L)
      .groupBy(counted => counted._2)
      .map(byCount => (byCount._1, byCount._2.map(_._1).min))

    results.collect().foreach(println)
  }
}
