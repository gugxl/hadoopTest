package com.gugu.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author gugu
 * @Classname ThreadPoolDemo
 * @Description 线程池测试类
 * @Date 2019/12/11 9:37
 */
public class ThreadPoolDemo {
    public static void main(String[] args) {
        //创建一个单线程的线程池
//        ExecutorService pool = Executors.newSingleThreadExecutor();
        //固定大小的线程池
//        ExecutorService pool = Executors.newFixedThreadPool(8);
        //可缓冲的线程词(可以有多个线程)
        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < 20; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    //打印当前线程的名字
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+" is over");
                }
            });
        }
        System.out.println("all Task is submitted");
//        pool.shutdown();
    }
}
