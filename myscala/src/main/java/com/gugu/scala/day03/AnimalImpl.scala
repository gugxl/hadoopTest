package com.gugu.scala.day03

class AnimalImpl(val name: String) {

}
object TestAnimal{
  def main(args: Array[String]): Unit = {
    val cat =  new AnimalImpl("cat") with Dog {

      override val name: String = "fox"

      override def run(): Unit = {
        println("----")
      }
    }
    println(cat.name)
  }
}