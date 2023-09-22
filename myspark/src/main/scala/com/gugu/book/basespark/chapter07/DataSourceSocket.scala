package com.gugu.book.basespark.chapter07

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}
import scala.io.Source

object DataSourceSocket {
    def index(length: Int) = { //返回位于0到length-1之间的一个随机数
        val rdm = new java.util.Random
        rdm.nextInt(length)
    }
    
    def main(args: Array[String]): Unit = {
        if (args.length != 1) {
            System.err.println("Usage: <port>")
            System.exit(1)
        }
        
        val fileName = "D:\\applicationfiles\\data\\ml-25m\\README.txt"
        val lines = Source.fromFile(fileName).getLines.toList
        val rowCount = lines.length
        val listener = new ServerSocket(args(0).toInt)
        
        while (true) {
            val socket: Socket = listener.accept()
            new Thread() {
                override def run(): Unit = {
                    
                    println("Got client connected from: " + socket.getInetAddress)
                    val writer = new PrintWriter(socket.getOutputStream, true)
                    while (true) {
                        Thread.sleep(100)
                        val content: String = lines(index(rowCount))
                        println(content)
                        writer.write(content + "\n")
                        writer.flush()
                    }
                }
            }
        }
    }
    
}
