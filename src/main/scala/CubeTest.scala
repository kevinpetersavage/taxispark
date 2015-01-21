import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class CubeTest extends FunSuite with BeforeAndAfterEach with ShouldMatchers {

  test("produces cubes"){
    val ns = List(8, 9, 10)

    println(ns.filterNot(n => Cubes.cubeRt(n).pow(3) == BigInt(n)))
  }


}