import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

object Taxi4 {
  def main(args: Array[String]): Unit = {
    val start = args(0).toInt
    val end = args(1).toInt

    val lowerLimitToTN = args(2).toInt
    val splits = args(3).toInt
    val range = Random.shuffle(Range(start, end).toList)

    val conf = new SparkConf().setAppName("taxi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val values = sc
      .parallelize(range, splits)

    val result = values.cartesian(values)
      .filter(pair => pair._1 <= pair._2)
      .flatMap(pair => {
        val firstSum: BigInt = (BigInt(pair._1).pow(3)) + (BigInt(pair._2).pow(3))

        (pair._1 until end).flatMap{value =>
          val sum = firstSum - (BigInt(value).pow(3))

          if (sum > 0) {
            val rootOfSum: BigInt = Cubes.cubeRt(sum)
            if (rootOfSum > pair._2 && (rootOfSum * rootOfSum * rootOfSum == sum)) {
              Option(firstSum)
            } else {
              None
            }
          } else {
            None
          }
        }
      })
      .countByValue().filter(_._2 >lowerLimitToTN)

    println(result)
  }

}
