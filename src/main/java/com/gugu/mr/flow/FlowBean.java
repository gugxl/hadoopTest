package com.gugu.mr.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author gugu
 * @Classname FlowBean
 * @Description TODO
 * @Date 2019/11/27 11:56
 */
public class FlowBean implements Writable {
    private int upFlow;
    private int downFlow;
    private int amountFlow;
    private String phone;

    public FlowBean() {
    }

    public FlowBean(String phone, int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.amountFlow = upFlow+ downFlow;
        this.phone = phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(amountFlow);
        dataOutput.writeUTF(phone);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.amountFlow = dataInput.readInt();
        this.phone = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", amountFlow=" + amountFlow +
                ", phone='" + phone + '\'' +
                '}';
    }
}
