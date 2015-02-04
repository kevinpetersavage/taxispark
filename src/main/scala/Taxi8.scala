import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Taxi8 {
  def main(args: Array[String]): Unit = {
    val start = 1
    val end = args(0).toInt

    val lowerLimitToTN = args(1).toInt
    val splits = args(2).toInt
    val range = Range(start, end)

    val conf = new SparkConf().setAppName("taxi").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val results = range.flatMap{ n =>
      val top = new LocationAndScore(n+1, n+1)
      val values = sc.parallelize(initialValues(n), splits)

      var newValues = values

      var rdds = List[RDD[LocationAndScore]]()
      while (newValues.count()>0){
        rdds = newValues :: rdds
        newValues = iterateValues(newValues, top)
      }

      val result = rdds.reduce(_ ++ _)
        .distinct()
        .groupBy(_.score)
        .map(group => (group._2.size, group._1))
        .collect()

      result
    }.groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2).min))


    results.foreach(println(_))
  }

  def iterateValues(values: RDD[LocationAndScore], top: LocationAndScore): RDD[LocationAndScore] = {
    values.flatMap { location =>
      var list = List[LocationAndScore]()
      val right = new LocationAndScore(location.xValue + 1, location.yValue)
      if (right.score < top.score) {
        list = right :: list
      }

      val down = new LocationAndScore(location.xValue, location.yValue + 1)
      if (down.xValue > down.yValue && down.score < top.score) {
        list = down :: list
      }

      list
    }
  }

  def initialValues(n: BigInt): List[LocationAndScore] = {
    var list = List[LocationAndScore]()
    var j = n
    var k = BigInt(0)
    do {
      list = new LocationAndScore(n + k, n - j) :: list
      k = k+1
      val s = Cubes.cubeRt((k * k * k) + (3 * k * k * n) + (3 * k * n * n) - (n * n * n)) + 1
      j = s + n
    } while (n-j > 0)
    list = new LocationAndScore(n + k, 1) :: list
    list.distinct
  }

  class LocationAndScore(x:BigInt, y:BigInt) extends Serializable{
    val score = (x*x*x)+(y*y*y)
    val xValue = x
    val yValue = y

    override def toString = s"$score from $xValue and $yValue"

    def canEqual(other: Any): Boolean = other.isInstanceOf[LocationAndScore]

    override def equals(other: Any): Boolean = other match {
      case that: LocationAndScore =>
        (that canEqual this) &&
          score == that.score &&
          xValue == that.xValue &&
          yValue == that.yValue
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(score, xValue, yValue)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

}
