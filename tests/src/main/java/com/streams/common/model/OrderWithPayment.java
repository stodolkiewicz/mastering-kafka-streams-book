package com.streams.common.model;

public class OrderWithPayment {
    private Order order;
    private Payment payment;
    private String joinStatus;

    public OrderWithPayment() {}

    public OrderWithPayment(Order order, Payment payment, String joinStatus) {
        this.order = order;
        this.payment = payment;
        this.joinStatus = joinStatus;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public Payment getPayment() {
        return payment;
    }

    public void setPayment(Payment payment) {
        this.payment = payment;
    }

    public String getJoinStatus() {
        return joinStatus;
    }

    public void setJoinStatus(String joinStatus) {
        this.joinStatus = joinStatus;
    }

    @Override
    public String toString() {
        return "OrderWithPayment{" +
                "order=" + order +
                ", payment=" + payment +
                ", joinStatus='" + joinStatus + '\'' +
                '}';
    }
}