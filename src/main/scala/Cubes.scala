import org.apache.zookeeper.KeeperException.UnimplementedException

object Cubes {
  def cubeRt(n: BigInt) : BigInt = {
    if (n < 0) throw new UnimplementedException
    if (n < 2) return Math.pow(n.toDouble, 1.0/3.0).floor.toInt
    var a : BigInt = 1
    var b = n
    while(b > a) {
      val mid = (a + b) / 2
      if(mid*mid*mid > n) {
        b = mid - 1
      } else {
        a = mid+1
      }
      if (a*a*a == n){
        return a
      }
      if (b*b*b == n){
        return b
      }
    }
    a-1
  }

  def isCube(n: BigInt): Boolean = {
    val root: BigInt = cubeRt(n)
    root*root*root == n
  }
}
