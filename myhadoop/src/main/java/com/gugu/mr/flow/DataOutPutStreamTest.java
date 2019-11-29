package com.gugu.mr.flow;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * @author gugu
 * @Classname DataOutPutStreamTest
 * @Description TODO
 * @Date 2019/11/27 12:06
 */
public class DataOutPutStreamTest {
    public static void main(String[] args) throws Exception {
        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream("D://a.txt"));
        dataOutputStream.write("小谷".getBytes("UTF-8"));
        dataOutputStream.close();
        DataOutputStream dataOutputStream2 = new DataOutputStream(new FileOutputStream("D://b.txt"));
        dataOutputStream2.writeUTF("小谷");
        dataOutputStream2.close();

    }
}
