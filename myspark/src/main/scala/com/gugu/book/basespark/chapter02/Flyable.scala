package com.gugu.book.basespark.chapter02

trait Flyable {
    var maxFlyHeight: Int //抽象字段
    
    def fly() //抽象方法
    
    def breathe() { //具体的方法
        println("I can breathe")
    }
}

trait HasLegs {
    val legs: Int //抽象字段
    
    def move() {
        printf("I can walk with %d legs", legs)
    }
}

class Animal(val category: String) {
    def info() {
        println("This is a " + category)
    }
    
}

class Bird(flyHeight: Int) extends Animal("Bird") with Flyable with HasLegs {
    override val legs: Int = 2
    override var maxFlyHeight: Int = flyHeight
    
    override def fly(): Unit = {
        printf("I can fly at the height of %d.\n", maxFlyHeight)
    }
}

object Bird {
    def main(args: Array[String]): Unit = {
        val b = new Bird(100)
        b.fly()
        b.info()
        b.breathe()
        b.move()
    }
}