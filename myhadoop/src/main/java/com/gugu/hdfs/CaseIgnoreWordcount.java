package com.gugu.hdfs;

/**
 * @author gugu
 * @Classname CaseIgnoreWordcount
 * @Description TODO
 * @Date 2019/11/26 18:41
 */
public class CaseIgnoreWordcount implements MyMapper {
    @Override
    public void map(String line, MyContext context) {
        String[] words = line.split(" ");
        for (String word:words) {
            word = word.toLowerCase();
            Object value = context.get(word);
            if (null == value){
                context.write(word, 1);
            }else {
                context.write(word, ((int) value)+ 1);
            }
        }
    }
}
