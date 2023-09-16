package com.gugu.book.basespark.chapter02

abstract class MyCar{
    val carBrand:String
    def info()
    def greeting(): Unit = {
        println("Welcome to my car")
    }
}

class BMWCar extends MyCar{
    override val carBrand: String = "BMW"
    
    override def info(): Unit = {
        printf("This is a %s car. It is cheap.\n", carBrand)
    }
    
    override def greeting(): Unit = {
        println("Welcome to BMW car")
    }
}

class BYDCar extends MyCar{
    override val carBrand: String = "BYD"
    
    override def info(): Unit = {
        printf("This is a %s car. It is cheap.\n", carBrand)
    }
    
    override def greeting(): Unit = {
        println("Welcome to BYD car")
    }
}

object MyCar {
    def main(args: Array[String]): Unit = {
        val myCar1 = new BMWCar
        var myCar2 = new BYDCar
        myCar1.greeting()
        myCar1.info()
        myCar2.greeting()
        myCar2.info()
    }
    
}
