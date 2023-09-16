package com.gugu.book.basespark.chapter02

class Counter2 {
    private var value = 0
    private var name = ""
    private var step = 1
    println("the main constructor")
    
    def this(name: String) {
        this()
        this.name = name
        println("the first auxiliary constructor,name:%s\n", name)
    }
    
    def this(name: String, step: Int) {
        this(name)
        this.step = step
        printf("the second auxiliary constructor,name:%s,step:%d\n", name, step)
        
    }
    
    def increment(step: Int): Unit = {
        value += step
    }
    
    def current(): Int = {
        value
    }
    
}

object Counter2 {
    def main(args: Array[String]): Unit = {
        var c = new Counter2("gugu")
        println(c.name)
        c.name = ("xiaogu")
        c.name = "xiaogu"
        println(c.name)
        
        var c2 = new Counter2("gugu", 5)
        println(c2)
        
    }
}