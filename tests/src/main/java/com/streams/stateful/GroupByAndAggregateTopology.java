package com.streams.stateful;

import com.streams.model.Order;
import com.streams.model.CustomerOrderSummary;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Tests GroupByAndAggregateTopology methods: groupBy() + aggregate()
 * 
 * groupBy() - groups orders by customerId (triggers repartitioning)
 * aggregate() - stateful operation that aggregates orders per customer
 *              using Initializer + Aggregator with different result type
 */
public class GroupByAndAggregateTopology {
    private static final Logger logger = LoggerFactory.getLogger(GroupByAndAggregateTopology.class);

    /**
     * Creates topology that groups orders by customerId and aggregates order statistics.
     * 
     * REPARTITIONING SCENARIO:
     * - groupBy() changes key from orderId to customerId
     * - Triggers automatic repartitioning via internal topic
     * - All orders for same customer end up in same partition
     * 
     * AGGREGATE LOGIC:
     * - Initializer: creates empty CustomerOrderSummary for new customer
     * - Aggregator: updates customer summary with each new order
     * - Only COMPLETED orders count towards totalAmount
     * - All orders count towards orderCount
     * - avgOrderValue is calculated automatically (logic in Order class)
     */
    public static Topology createGroupByAndAggregateTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Order> ordersStream = builder.stream(
                "orders-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(Order.class))
        );

        /*
         * groupBy() creates new key from order content:
         * 
         * Key Change: orderId → customerId
         * - Original: key="order-123", value=Order{customerId="cust-1", amount=100.0, status="COMPLETED"}
         * - After groupBy: key="cust-1", value=Order{customerId="cust-1", amount=100.0, status="COMPLETED"}
         * 
         * Repartitioning Required:
         * - Orders for same customer must be in same partition for aggregation
         * - Kafka automatically creates internal repartition topic
         */
        KGroupedStream<String, Order> streamGroupedByCustomerId = ordersStream.groupBy(
                (customerIdKey, order) -> order.getCustomerId() != null ? order.getCustomerId() : "UNKNOWN",
                Grouped.with(Serdes.String(), new JsonSerDes<>(Order.class))
        );

        /*
         * aggregate() - stateful operation with two functions:
         * 
         * 1. Initializer: Creates fresh aggregate for new customerId
         *    - Called once per new key (customerId)
         *    - Returns: empty CustomerOrderSummary with zero values
         * 
         * 2. Aggregator: Updates aggregate with each new order
         *    - Called for every order record
         *    - Parameters: (key, newValue, currentAggregate)
         *    - Business logic: sum COMPLETED orders, count all orders
         *    - Returns: updated CustomerOrderSummary
         */
        Initializer<CustomerOrderSummary> aggregateInitializer =
                () -> new CustomerOrderSummary(null, 0.0, 0);

        Aggregator<String, Order, CustomerOrderSummary> aggregator =
                (customerId, newOrder, currentSummary) -> {
                    // Set customer ID in aggregate
                    currentSummary.setCustomerId(customerId);

                    // Null-safe extraction of current values
                    Double currentTotal = currentSummary.getTotalAmount() != null ? currentSummary.getTotalAmount() : 0.0;
                    Integer currentCount = currentSummary.getOrderCount() != null ? currentSummary.getOrderCount() : 0;

                    // Business rule: only COMPLETED orders contribute to revenue
                    if ("COMPLETED".equals(newOrder.getStatus()) && newOrder.getAmount() != null) {
                        currentSummary.setTotalAmount(currentTotal + newOrder.getAmount());
                    }
                    
                    // All orders count towards total order count
                    currentSummary.setOrderCount(currentCount + 1);
                    
                    return currentSummary;
                };

        KTable<String, CustomerOrderSummary> customerOrderSummaryKTable = streamGroupedByCustomerId.aggregate(
                aggregateInitializer,
                aggregator,
                Materialized.<String, CustomerOrderSummary, KeyValueStore<Bytes, byte[]>>as("customer-order-summary-store")
        );

        // Convert KTable changelog to KStream and output to topic
        customerOrderSummaryKTable
                .toStream()
                .to("customer-summaries-topic",  Produced.with(Serdes.String(), new JsonSerDes<>(CustomerOrderSummary.class)));

        return builder.build();
    }
}