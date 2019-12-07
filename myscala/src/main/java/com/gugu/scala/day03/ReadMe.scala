package com.gugu.scala.day03

/**
 * Array ArrayBuffer
 *
 * 集合
 *     可变集合（collection.mutable）
 *         ListBuffer => 内容和长度都可以改变
 *
 *     不可变集合(collection.immutable)
 *         List => 长度和内容都不可变
 *         var list = List(1,3,4)
 *
 *
 *     Map
 *         Map[String, String]("a" -> "a", "b" -> "b")
 *         get => Option[String]
 *         getOrElse("key", defaultValue) => String
 *
 *     Set
 *         存储的元素是无序的，且里面的元素是没有重复的
 *
 *     元组中可以分任意类型的数据, 最多可以放22个
 *         (1, true, "", Object)
 *         获取元组中的元素 tuple._2
 *
 *     Seq Nil
 *         Seq 中分为head tail
 *         第一个元素就是head
 *         剩余的都是tail
 *         List(9) head=9 tail=Nil
 *
 *     Option Some None
 *         Some 和 None都是Option子类
 *         获取Some中的值是通过他的get方法
 *         None
 *
 *     集合相关的API操作 101
 *         aggregate()(seqOp, combOp) 对集合进行某种聚合操作
 *         count(boolean) 返回是符合条件的元素个数
 *         diff    某个集合和另外一个集合的差集
 *         distinct 对集合中的元素进行去重
 *         filter(boolean) 过滤出符合条件的元素集合
 *         flatMap  对集合进行某种映射（map）操作，然后在进行扁平化操作（flatten）
 *         flatten 扁平化操作
 *         fold()() 折叠操作
 *         foldLeft()() 从左到右折叠
 *         foldRight()()
 *         foreach(f: A => Unit) 遍历集合
 *         groupBy(key) 按照key进行分组
 *         grouped(Int) 将集合按照Int个数进行分组
 *         head 获取集合中的头元素
 *         indices 返回集合的角标范围
 *         intersect 请求两个集合的交集
 *         length 返回集合的元素个数
 *         map 对集合进行某种映射操作
 *         mkString 对集合进行格式化输出
 *         nonEmpty 判断集合是否为空
 *         reduce 聚合
 *
 *         reverse 将集合进行反转
 *         size 返回集合的长度
 *         slice(start, end) 截取集合的元素
 *         sortBy(key) 集合按照某个key进行排序
 *         sortWith(boolean) 将集合按照某种规则进行排序
 *         sorted 集合按照升序排序
 *         sum 对集合进行求和操作
 *         tail 返回集合的尾部元素列表
 *         zip 拉链操作 相同角标位置的元素组合到一起，返回一个新的集合
 */
object ReadMe {
  def main(args: Array[String]): Unit = {
    println(ScalaStatic.name)
    ScalaStatic.age = 20
    println(ScalaStatic.age)
    ScalaStatic.saySomething("今天有点冷")
    ScalaStatic.apply("西红柿炒番茄")
    // 默认调用就是apply方法
    ScalaStatic("油焖大虾")// 语法糖（sugar）
  }
}
/**
 * object
 *     单例的对象，里面定义的成员变量 和 方法都是静态的（static）
 *
 *     伴生对象：
 *         当object 的名称和类的名称一致的时候，这个对象叫着这个类的伴生对象（必须在同一个文件中）
 *
 *     apply
 *         object对象() -> object中的apply()方法
 *
 * class
 *     类
 *
 *     类的主构造器：定义在类的名称后面的构造器
 *     类的辅助构造器：定义在类体中，def this()类的辅助构造器，在类的辅助构造器中必须先调用类的主构造器
 *     类的成员变量：
 *         var 对外提供了get 和 set方法
 *         val 对外值只提供了get方法
 *
 *
 *     访问权限：
 *         成员变量 private[] 标识外部没法访问它get 或者 set方法
 *         方法    private[] 标识外部没法调用这个方法
 *         构造器  private[] 在外部无法访问
 *         类的    private[包名] 标识这个类在这个包名及其子包下可见（可访问）
 *                 private[this] 只能在当前包下可见子包不可见（不能访问）
 *
 *
 *      伴生对象可以访问类中的私有方法private, 不能访问private[this]修饰的成员变量及方法
 *
 * 在scala中，final修饰的
 * 类： 类不能被集成
 * 方法：方法不能被override
 * 成员变量: 不能被重新修改（赋值）
 *
 * type
 * alias
 */

