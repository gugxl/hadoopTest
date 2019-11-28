package com.gugu.mr.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author gugu
 * @Classname JoinBean
 * @Description TODO
 * @Date 2019/11/28 10:03
 */
public class JoinBean implements Writable {

    private String orderId;
    private String userId;
    private String userName;
    private int userAge;
    private String userFriend;
    private String tableName;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.userName);
        dataOutput.writeInt(this.userAge);
        dataOutput.writeUTF(this.userFriend);
        dataOutput.writeUTF(this.tableName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.userName = dataInput.readUTF();
        this.userAge = dataInput.readInt();
        this.userFriend = dataInput.readUTF();
        this.tableName = dataInput.readUTF();
    }

    public void set(String orderId, String userId, String userName, int userAge, String userFriend, String tableName) {
        this.orderId = orderId;
        this.userId = userId;
        this.userName = userName;
        this.userAge = userAge;
        this.userFriend = userFriend;
        this.tableName = tableName;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getUserAge() {
        return userAge;
    }

    public void setUserAge(int userAge) {
        this.userAge = userAge;
    }

    public String getUserFriend() {
        return userFriend;
    }

    public void setUserFriend(String userFriend) {
        this.userFriend = userFriend;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "JoinBean{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", userName='" + userName + '\'' +
                ", userAge=" + userAge +
                ", userFriend='" + userFriend + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
