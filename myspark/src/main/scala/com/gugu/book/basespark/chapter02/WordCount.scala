package com.gugu.book.basespark.chapter02

import java.io.File
import scala.io.Source

object WordCount {
    def main(args: Array[String]): Unit = {
        
        val dirfile = new File("D:\\applicationfiles\\data")
        val files = dirfile.listFiles
        var results = Map.empty[String, Int]
        for (file <- files) {
            val lines: Iterator[String] = Source.fromFile(file).getLines()
            val strs: Iterator[String] = lines.flatMap(line => line.split(" "))
            strs foreach { word: String =>
                results = results + (word -> (results.getOrElse(word, 0) + 1))
            }
            
            
        }
        results foreach { case (k, v) => println(s"$k:$v") }
    }
    
}
