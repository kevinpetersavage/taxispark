import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class CubeTest extends FunSuite with BeforeAndAfterEach with ShouldMatchers {
  test("produces cubes"){
    assert(List(8, 27).filterNot(n => Cubes.cubeRt(n).pow(3) == BigInt(n)).isEmpty)
  }

  test("rounds down cubes"){
    assert(List(9, 28).filterNot(n => Cubes.cubeRt(n).pow(3) < BigInt(n)).isEmpty)
  }

  test("checks cubes"){
    assert(List(8,9,10,27).filter(n => Cubes.isCube(n)) == List(8,27))
  }

  test("checks cubes example"){
    assert(Cubes.cubeRt(1000) == BigInt(10))
  }
}