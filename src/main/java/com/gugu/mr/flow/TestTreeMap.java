package com.gugu.mr.flow;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author gugu
 * @Classname TestTreeMap
 * @Description TODO
 * @Date 2019/11/27 13:41
 */
public class TestTreeMap {
    public static void main(String[] args) {
        TreeMap<FlowBean, String> treeMap = new TreeMap<>(new Comparator<FlowBean>() {
            @Override
            public int compare(FlowBean o1, FlowBean o2) {
                if(0 == o1.getAmountFlow()-o2.getAmountFlow()){
                    return o1.getPhone().compareTo(o2.getPhone());
                }
                return o1.getAmountFlow()-o2.getAmountFlow();
            }
        });
//        treeMap.put("a",1);
//        treeMap.put("b",2);
//        treeMap.put("aa",3);
//        treeMap.put("ab",4);
//        treeMap.put("ba",1);
//        treeMap.put("bba",1);
//        Set<Map.Entry<String, Integer>> entries = treeMap.entrySet();
//        for (Map.Entry<String, Integer> entry : entries){
//            System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
//        }
        FlowBean flowBean1 = new FlowBean("1573699", 400, 500);
        FlowBean flowBean2 = new FlowBean("1573688", 500, 400);
        FlowBean flowBean3 = new FlowBean("1573677", 600, 100);
        FlowBean flowBean4 = new FlowBean("1573666", 700, 200);
        FlowBean flowBean5 = new FlowBean("1573655", 800, 900);
        treeMap.put(flowBean1,null);
        treeMap.put(flowBean2,null);
        treeMap.put(flowBean3,null);
        treeMap.put(flowBean4,null);
        treeMap.put(flowBean5,null);

        Set<Map.Entry<FlowBean, String>> entries = treeMap.entrySet();
        for (Map.Entry<FlowBean, String> entry : entries){
            System.out.println("key:"+((FlowBean)entry.getKey()).toString());
        }
    }
}
