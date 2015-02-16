import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.{PutAttributesRequest, ReplaceableAttribute, SelectRequest}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object Taxi8 {

  def main(args: Array[String]): Unit = {
    val processedTableName = "processed"

    val interval = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val key = args(3)
    val secret = args(4)

    val db = new AmazonSimpleDBClient(new BasicAWSCredentials(key, secret))

    val coveredValues = findCoveredValues(processedTableName, db)

    val conf = new SparkConf().setAppName("taxi").setMaster("local[4]")
    val sc = new SparkContext(conf)

    Stream.from(1).map(_*interval)
      .filter(n => (n until n+interval).forall(coveredValues.contains))
      .foreach{ n =>
        val maxK = calculateMaxK(n, interval)
        val kRange = sc.parallelize(0 until maxK, splits)

        kRange.flatMap(producePairsFor(n, _, interval))
          .map(performSumOfCubes)
          .groupBy(identity)
          .map(group => (group._2.size, group._1))
          .filter(pair => pair._1 >= lowerLimitToTN)
          .groupBy(_._1)
          .map(pair => (pair._1, pair._2.map(_._2).min))
          .collect()
          .foreach(putToDb(db, "results", "count"))

        putToDb(db, processedTableName, "end")(n, n+interval)
      }
  }

  def findCoveredValues(processedTableName: String, db: AmazonSimpleDBClient) = {
    db.select(new SelectRequest(s"select * from $processedTableName"))
      .getItems
      .flatMap(item => item.getAttributes.map(attribute => (attribute.getValue, item.getName)))
      .map(item => Range(item._1.toInt, item._2.toInt))
      .reduce(_.union(_))
      .toSet
  }

  def putToDb[T,U](db: AmazonSimpleDBClient, table: String, attributeName: String)(keyValue: (T, U)): Unit = {
    val attributes = List(new ReplaceableAttribute(attributeName, keyValue._1.toString, true))
    val putRequest = new PutAttributesRequest(table, keyValue._2.toString, attributes)
    db.putAttributes(putRequest)
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
