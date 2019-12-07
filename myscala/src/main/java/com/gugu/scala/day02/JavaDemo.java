package com.gugu.scala.day02;

/**
 * @author gugu
 * @Classname JavaDemo
 * @Description TODO
 * @Date 2019/12/7 17:19
 */
public class JavaDemo {
    public static void main(String[] args) {
        main2("a","b","c");
    }
    public static void main2(String x, String ... other){
        System.out.println(x);
        System.out.println(other[0]);
        System.out.println(other[1]);
    }
}
