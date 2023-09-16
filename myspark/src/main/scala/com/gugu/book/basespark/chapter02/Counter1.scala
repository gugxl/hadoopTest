package com.gugu.book.basespark.chapter02

class Counter1 {
    var value = 0
    
    def increment(step: Int): Unit = {
        value += step
    }
    
    def current: Int = value
    
    def getValue(): Int = value
    
}

object Counter1{
    
    
    def main(args: Array[String]): Unit = {
        val c: Counter1 = new Counter1
        c increment 5
        println(c.getValue())
        println(c.getValue)
        println(c.current)
    }
}