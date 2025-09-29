package com.streams.stateful;

import com.streams.model.Order;
import com.streams.model.CustomerOrderSummary;
import com.streams.serde.JsonDeserializer;
import com.streams.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/*
 * Tests GroupByAndAggregateTopology methods: groupBy() + aggregate()
 * 
 * aggregate() - stateful operation with Initializer + Aggregator
 * Different from reduce() - can return different type than input
 */
class GroupByAndAggregateTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> inputTopic;
    private TestOutputTopic<String, CustomerOrderSummary> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testGroupByAndAggregateApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
        Topology topology = GroupByAndAggregateTopology.createGroupByAndAggregateTopology();
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "orders-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "customer-summaries-topic",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(CustomerOrderSummary.class)
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testAggregateOrders_shouldSumCompletedOrdersOnly() {
        // given - customer with mixed order statuses
        final String CUSTOMER_ID = "cust-1";
        
        // TODO: Create orders for same customer:
        // - order1: amount=100.0, status="COMPLETED"
        // - order2: amount=50.0, status="PENDING" 
        // - order3: amount=75.0, status="COMPLETED"
        
        // Expected: totalAmount=175.0 (only COMPLETED), orderCount=3 (all orders)

        // when
        // TODO: Send orders to inputTopic

        // then
        // TODO: Verify output results
        // Should have 3 results (one for each order processed)
        // Final result should have:
        // - customerId="cust-1"
        // - totalAmount=175.0 
        // - orderCount=3
        // - avgOrderValue=58.33 (175/3)
    }

    @Test
    void testAggregateOrders_shouldHandleMultipleCustomers() {
        // given
        final String CUSTOMER_A = "cust-A";
        final String CUSTOMER_B = "cust-B";
        
        // TODO: Create orders for different customers:
        // Customer A: 2 orders (both COMPLETED)
        // Customer B: 1 order (COMPLETED)
        
        // when
        // TODO: Send orders to inputTopic

        // then
        // TODO: Verify both customers have separate aggregations
        // Should have 3 total results
        // Verify final states for both customers
    }

    @Test
    void testAggregateOrders_shouldUseInitializerForNewCustomer() {
        // given
        final String NEW_CUSTOMER = "new-customer";
        
        // TODO: Create first order for new customer
        // amount=200.0, status="COMPLETED"

        // when
        // TODO: Send order to inputTopic

        // then
        // TODO: Verify Initializer was called
        // First result should show:
        // - customerId="new-customer"
        // - totalAmount=200.0
        // - orderCount=1
        // - avgOrderValue=200.0
    }

    @Test
    void testAggregateOrders_shouldHandleNullCustomerId() {
        // given
        // TODO: Create order with null customerId
        // Should be grouped under "UNKNOWN" key

        // when
        // TODO: Send order to inputTopic

        // then
        // TODO: Verify order is processed under "UNKNOWN" key
    }
}