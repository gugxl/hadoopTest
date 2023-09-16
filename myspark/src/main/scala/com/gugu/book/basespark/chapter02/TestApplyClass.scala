package com.gugu.book.basespark.chapter02

class TestApplyClass {
    def apply(param: String) {
        println("apply method called: " + param)
    }
    
}

object TestApplyClass {
    def main(args: Array[String]): Unit = {
        val t = new TestApplyClass
        t("gugu")
    }
}
