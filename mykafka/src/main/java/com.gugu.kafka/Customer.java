package com.gugu.kafka;

public class Customer {
    private int customeId;
    private String customerName;

    public Customer(int customeId, String customerName) {
        this.customeId = customeId;
        this.customerName = customerName;
    }

    public int getCustomeId() {
        return customeId;
    }

    public void setCustomeId(int customeId) {
        this.customeId = customeId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }
}
