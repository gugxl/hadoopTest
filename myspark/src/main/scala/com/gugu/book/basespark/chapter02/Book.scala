package com.gugu.book.basespark.chapter02

case class Book(val name: String, val price: Double)

object Book {
    def main(args: Array[String]): Unit = {
        val books = Map("hadoop" -> Book("Hadoop", 35.5), "spark" -> Book("Spark", 55.5), "hbase" -> Book("Hbase", 26.0))
        println(books.get("hadoop"))
        println(books.get("hive"))
        println(books.get("hadoop").get)
        //        println(books.get("hive").get) //NoSuchElementException
        println(books.get("hive").getOrElse(Book("Unknown name", 0)))
    }
}
