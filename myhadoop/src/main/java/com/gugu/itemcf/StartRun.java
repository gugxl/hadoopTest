package com.gugu.itemcf;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gugu
 * @Classname StartRun
 * @Description TODO
 * @Date 2019/12/25 9:49
 */
public class StartRun {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.set("mapeduce.app-submission.cross-paltform","true");
        conf.set("mapreduce.framework.name","local");

        //所有mr的输入和输出目录定义在map集合中
        Map<String, String> paths = new HashMap<String, String>();
        paths.put("Step1Input", "file:///D:\\logs\\sxt\\itemcf\\input");
        paths.put("Step1Output", "file:///D:\\logs\\sxt\\itemcf\\step1");
        paths.put("Step2Input", paths.get("Step1Output"));
        paths.put("Step2Output", "file:///D:\\logs\\sxt\\itemcf\\step2");
        paths.put("Step3Input", paths.get("Step2Output"));
        paths.put("Step3Output", "file:///D:\\logs\\sxt\\itemcf\\step3");
        paths.put("Step4Input1", paths.get("Step2Output"));
        paths.put("Step4Input2", paths.get("Step3Output"));
        paths.put("Step4Output", "file:///D:\\logs\\sxt\\itemcf\\step4");
        paths.put("Step5Input", paths.get("Step4Output"));
        paths.put("Step5Output", "file:///D:\\logs\\sxt\\itemcf\\step5");
        paths.put("Step6Input", paths.get("Step5Output"));
        paths.put("Step6Output", "file:///D:\\logs\\sxt\\itemcf\\step6");

        Step1.run(conf,paths);
        Step2.run(conf,paths);
		Step3.run(conf, paths);
		Step4.run(conf, paths);
		Step5.run(conf, paths);
		Step6.run(conf, paths);
    }

    public static Map<String,Integer> R = new HashMap<>();
    static {
        R.put("click", 1);
        R.put("collect", 2);
        R.put("cart", 3);
        R.put("alipay", 4);
    }
}
