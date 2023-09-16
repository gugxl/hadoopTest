package com.gugu.book.basespark.chapter02

class Box[T] {
    var elems: List[T] = Nil
    
    def pop: Option[T] = {
        if (elems.isEmpty) None else {
            val temp = elems.head
            elems = elems.tail
            Some(temp)
        }
    }
    
    def push(a: T) = {
        elems = a :: elems
    }
}
case class Book2(name:String)

object Box{
    def main(args: Array[String]): Unit = {
        val a = new Box[Int] //实例化Box类时，T的具体类型为Int
        a.push(23)
        println(a.pop)
        val b = new Box[Book2]  //实例化Box时，T的具体类型为自定义的Book类型
        b.push(Book2("Hadoop"))
        println(b.pop)
    }
}