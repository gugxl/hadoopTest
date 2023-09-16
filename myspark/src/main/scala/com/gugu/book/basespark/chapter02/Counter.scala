package com.gugu.book.basespark.chapter02

class Counter {
    private var privateValue = 0
    
    def value: Int = privateValue
    
    def value_(newValue: Int): Unit = {
        if (newValue > 0) {
            privateValue = newValue
        }
    }
    
    def current(): Int = {
        value
    }
}

object Counter {
    def main(args: Array[String]): Unit = {
        val myCount = new Counter
        myCount.value_(3)
        println(myCount.value)
    }
}
