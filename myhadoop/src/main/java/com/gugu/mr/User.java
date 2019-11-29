package com.gugu.mr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author gugu
 * @Classname User
 * @Description TODO
 * @Date 2019/11/23 13:54
 */
public class User implements WritableComparable<User> {
    String userName;
    int age;
    int score;

    public void set(String userName, int age, int score) {
        this.userName = userName;
        this.age = age;
        this.score = score;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userName);
        dataOutput.writeInt(age);
        dataOutput.writeInt(score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userName = dataInput.readUTF();
        age = dataInput.readInt();
        score = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "User{" +
                "userName='" + userName + '\'' +
                ", age=" + age +
                ", score=" + score +
                '}';
    }

    @Override
    public int compareTo(User user) {
        return score - user.score;
    }


}
