package com.gugu.scala.day05

import java.io.File


object MyImpicits {
  /**
   * 定义了一个隐式方法，将file类型变成RichFile
   */
  implicit def file2RichFile(file : File) = new RichFile(file)

  // 隐式将Student -> Ordered[Student]
implicit def student2OrderedStu(stu: Students) = new Ordered[Students]{
  override def compare(that: Students): Int = stu.age - that.age
}
  // 一个隐式对象实例 -> 一个又具体实现的Comparator
implicit val compareStu = new Ordering[Students]{
  override def compare(x: Students, y: Students): Int = x.age - y.age
}
}
