package com.gugu.hdfs;

/**
 * @author gugu
 * @Classname WordCountMapper
 * @Description TODO
 * @Date 2019/11/26 18:00
 */
public class WordCountMapper implements MyMapper {
    @Override
    public void map(String line, MyContext context) {
        String[] words = line.split(" ");
        for (String word:words) {
            Object value = context.get(word);
            if (null == value){
                context.write(word, 1);
            }else {
                context.write(word, ((int) value)+ 1);
            }
        }
    }
}
