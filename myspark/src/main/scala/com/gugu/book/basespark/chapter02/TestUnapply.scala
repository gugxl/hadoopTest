package com.gugu.book.basespark.chapter02

object TestUnapply {
    def main(args: Array[String]) {
        var Car2(carbrand, carprice) = Car2("gugu", 800000)
        println("brand: " + carbrand + " and carprice: " + carprice)
    }
}

class Car2(val brand: String, val price: Int) {
    def info() {
        println("Car brand is " + brand + " and price is " + price)
    }
}

object Car2 {
    def apply(brand: String, price: Int) = {
        println("Debug:calling apply ... ")
        new Car2(brand, price)
    }
    
    def unapply(c: Car2): Option[(String, Int)] = {
        println("Debug:calling unapply ... ")
        Some((c.brand, c.price))
    }
}