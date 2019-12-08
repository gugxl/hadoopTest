package com.gugu.scala.day05;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author gugu
 * @Classname Appcmp
 * @Description TODO
 * @Date 2019/12/8 14:38
 */
public class Appcmp {
    /**
     * @Description 在Scala中对应Comparable的是Ordered这个特质
     * @params
     * @param args
     * @return void
     * @auther gugu
     */

    public static void main(String[] args) {
        Teachers dd = new Teachers("丁丁", 99);
        Teachers jj = new Teachers("静静", 100);
        List<Teachers> list = new ArrayList<>();
        list.add(dd);
        list.add(jj);
        Collections.sort(list);

        for (Teachers teacher:list) {
            System.out.println("teachers = " + teacher.getName());
        }
    }
}
