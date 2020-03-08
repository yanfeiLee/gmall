object TestString {
  def main(args: Array[String]): Unit = {
    val str = "18823990000"
    println(str.take(3)+"****"+str.takeRight(4))

    println(str)
  }
}
