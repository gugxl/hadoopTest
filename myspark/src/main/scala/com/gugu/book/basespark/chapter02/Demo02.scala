package com.gugu.book.basespark.chapter02

object Demo02 {
    def main(args: Array[String]): Unit = {
        val counter = (_: Int) + 1
        val add = (_: Int) + (_: Int)
        val m1 = List(1, 2, 3)
        
        val m2 = m1.map(_ * 2)
        
    }
    
    
}
