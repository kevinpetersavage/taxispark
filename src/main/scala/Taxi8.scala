import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

object Taxi8 {
  val processedTableName = "processed"
  val resultsTableName = "results"

  def main(args: Array[String]): Unit = {

    val interval = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val key = args(3)
    val secret = args(4)

    val db = new AmazonSimpleDBClient(new BasicAWSCredentials(key, secret))
    createDomainsIfNeeded(db)

    val maxCovered = maxCoveredValue(processedTableName, db)

    val conf = new SparkConf().setAppName("taxi")
    val sc = new SparkContext(conf)

    Stream.from(0).map(_*interval).map(_+1)
      .filter { n => (n + interval - 1) > maxCovered}
      .foreach{ n =>
        println("about to do " + n)
        val maxK = calculateMaxK(n, interval)
        val kRange = sc.parallelize((0 until maxK).reverse, splits)

        kRange.flatMap(producePairsFor(n, _, interval))
          .map(performSumOfCubes)
          .groupBy(identity)
          .map(group => (group._2.size, group._1))
          .filter(pair => pair._1 >= lowerLimitToTN)
          .groupBy(_._1)
          .map(pair => (pair._2.map(_._2).min, pair._1))
          .collect()
          .foreach(putToDb(db, resultsTableName, "count"))

        putToDb(db, processedTableName, "end")(n, n+interval)
      }
  }

  def createDomainsIfNeeded(db: AmazonSimpleDBClient): Unit = {
    val domains: ListDomainsResult = db.listDomains()
    createDomainIfNeeded(db, domains, resultsTableName)
    createDomainIfNeeded(db, domains, processedTableName)
  }

  def createDomainIfNeeded(db: AmazonSimpleDBClient, domains: ListDomainsResult, name: String): Unit = {
    if (!domains.getDomainNames.contains(name)) {
      db.createDomain(new CreateDomainRequest(name))
    }
  }

  def maxCoveredValue(processedTableName: String, db: AmazonSimpleDBClient) = {
    val ranges: mutable.Buffer[Range] = db.select(new SelectRequest(s"select * from $processedTableName"))
      .getItems
      .flatMap(item => item.getAttributes.map(attribute => (item.getName,attribute.getValue)))
      .map(item => Range(item._1.toInt, item._2.toInt))
    if (ranges.isEmpty){
      0
    } else {
      ranges.map(_.max).max
    }
  }

  def putToDb[T,U](db: AmazonSimpleDBClient, table: String, attributeName: String)(keyValue: (T, U)): Unit = {
    val attributes = List(new ReplaceableAttribute(attributeName, keyValue._2.toString, true))
    val putRequest = new PutAttributesRequest(table, keyValue._1.toString, attributes)
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
