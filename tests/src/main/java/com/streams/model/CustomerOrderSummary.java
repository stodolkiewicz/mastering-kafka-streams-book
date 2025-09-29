package com.streams.model;

public class CustomerOrderSummary {
    private String customerId;
    private Double totalAmount;
    private Integer orderCount;
    private Double avgOrderValue;

    public CustomerOrderSummary() {}

    public CustomerOrderSummary(String customerId, Double totalAmount, Integer orderCount) {
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.orderCount = orderCount;
        this.avgOrderValue = calculateAvgOrderValue();
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
        this.avgOrderValue = calculateAvgOrderValue();
    }

    public Integer getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Integer orderCount) {
        this.orderCount = orderCount;
        this.avgOrderValue = calculateAvgOrderValue();
    }

    public Double getAvgOrderValue() {
        return avgOrderValue;
    }

    private Double calculateAvgOrderValue() {
        if (orderCount == null || orderCount == 0 || totalAmount == null) {
            return 0.0;
        }
        return totalAmount / orderCount;
    }

    @Override
    public String toString() {
        return "CustomerOrderSummary{" +
                "customerId='" + customerId + '\'' +
                ", totalAmount=" + totalAmount +
                ", orderCount=" + orderCount +
                ", avgOrderValue=" + avgOrderValue +
                '}';
    }
}