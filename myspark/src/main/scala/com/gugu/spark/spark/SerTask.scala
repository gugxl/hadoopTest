package com.gugu.spark.spark

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

class MapTask extends Serializable{
  //以后重哪里了读取数据

  //以后该如何执行，根据RDD的转换关系（调用那个方法，传入了什么函数）
  def m1(path:String) ={
    path.toString
  }
  def m2(line:String)={
    line.split(" ")
  }
}
object MapTask {
  def main(args: Array[String]): Unit = {
    //new一个实例，然后打印她的hashcode值

    //在Driver端创建这个实例

    //序列化后发生出去，发生个Executor，Executor接收后，反序列化，用一个实现了Runnable接口一个类包装一下，然后丢到线程池中
    val task = new MapTask
    println(task)
    val oos = new ObjectOutputStream(new FileOutputStream("./t"))
    oos.writeObject(task)
    oos.flush()
    oos.close()

    val ois1 = new ObjectInputStream(new FileInputStream("./t"))
    val o1: AnyRef = ois1.readObject()
    println(o1)
    ois1.close()

    val ois2 = new ObjectInputStream(new FileInputStream("./t"))
    val o2: AnyRef = ois2.readObject()
    println(o2)
    ois1.close()
  }

}
