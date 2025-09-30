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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.within;
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
        final Double EXPECTED_TOTAL_AMOUNT = 175.0;

        final String CUSTOMER_ID = "cust-1";

        final String ORDER_ID_1 = "orderId1";
        final String ORDER_STATUS_1 = "COMPLETED";
        final Double AMOUNT_ORDER_1 = 100.0;

        final String ORDER_ID_2 = "orderId2";
        final String ORDER_STATUS_2 = "PENDING";
        final Double AMOUNT_ORDER_2 = 50.0;

        final String ORDER_ID_3 = "orderId3";
        final String ORDER_STATUS_3 = "COMPLETED";
        final Double AMOUNT_ORDER_3 = 75.0;

        Order order1 = new Order(ORDER_ID_1, CUSTOMER_ID, AMOUNT_ORDER_1, ORDER_STATUS_1);
        Order order2 = new Order(ORDER_ID_2, CUSTOMER_ID, AMOUNT_ORDER_2, ORDER_STATUS_2);
        Order order3 = new Order(ORDER_ID_3, CUSTOMER_ID, AMOUNT_ORDER_3, ORDER_STATUS_3);

        // Expected: totalAmount=175.0 (only COMPLETED), orderCount=3 (all orders)

        // when
        // TODO: Send orders to inputTopic
        inputTopic.pipeInput(CUSTOMER_ID, order1);
        inputTopic.pipeInput(CUSTOMER_ID, order2);
        inputTopic.pipeInput(CUSTOMER_ID, order3);

        // then

        Set<String> producedTopicNames = testDriver.producedTopicNames();
        List<KeyValue<String, CustomerOrderSummary>> summaries = outputTopic.readKeyValuesToList();

        assertEquals(3, summaries.size());

        // second summary
        assertEquals(100.0, summaries.get(1).value.getTotalAmount());
        assertEquals(50.0, summaries.get(1).value.getAvgOrderValue());

        // third summary
        assertEquals(175.0, summaries.get(2).value.getTotalAmount());
        assertThat(summaries.get(2).value.getAvgOrderValue()).isCloseTo(58.33333, within(1e-5));

        // test value from KeyValueStore
        // KeyValueStore - always holds exactly 1 value per key
        CustomerOrderSummary customerOrderSummaryFromKeyValueStore =
                (CustomerOrderSummary) testDriver.getKeyValueStore("customer-order-summary-store").get("cust-1");
        assertEquals(175.0, customerOrderSummaryFromKeyValueStore.getTotalAmount());
        assertThat(customerOrderSummaryFromKeyValueStore.getAvgOrderValue()).isCloseTo(58.33333, within(1e-5));
    }

    @Test
    void testAggregateOrders_shouldHandleMultipleCustomers() {
        // given
        final String CUSTOMER_A = "cust-A";
        final String CUSTOMER_B = "cust-B";
        
        Order orderA1 = new Order("order-A1", CUSTOMER_A, 120.0, "COMPLETED");
        Order orderA2 = new Order("order-A2", CUSTOMER_A, 80.0, "COMPLETED");
        Order orderB1 = new Order("order-B1", CUSTOMER_B, 200.0, "COMPLETED");
        
        // when
        inputTopic.pipeInput("key-A1", orderA1);
        inputTopic.pipeInput("key-B1", orderB1);
        inputTopic.pipeInput("key-A2", orderA2);

        // then
        List<KeyValue<String, CustomerOrderSummary>> summaries = outputTopic.readKeyValuesToList();
        assertEquals(3, summaries.size());
        
        // First order for Customer A
        assertEquals(CUSTOMER_A, summaries.get(0).key);
        assertEquals(120.0, summaries.get(0).value.getTotalAmount());
        assertEquals(1, summaries.get(0).value.getOrderCount());
        assertEquals(120.0, summaries.get(0).value.getAvgOrderValue());
        
        // Customer B (separate aggregation)
        assertEquals(CUSTOMER_B, summaries.get(1).key);
        assertEquals(200.0, summaries.get(1).value.getTotalAmount());
        assertEquals(1, summaries.get(1).value.getOrderCount());
        assertEquals(200.0, summaries.get(1).value.getAvgOrderValue());
        
        // Customer A final state (after second order)
        assertEquals(CUSTOMER_A, summaries.get(2).key);
        assertEquals(200.0, summaries.get(2).value.getTotalAmount());
        assertEquals(2, summaries.get(2).value.getOrderCount());
        assertEquals(100.0, summaries.get(2).value.getAvgOrderValue());
    }

    @Test
    void testAggregateOrders_shouldHandleNullCustomerId() {
        // given
        Order orderWithNullCustomer = new Order("order-null", null, 150.0, "COMPLETED");

        // when
        inputTopic.pipeInput("order-key", orderWithNullCustomer);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, CustomerOrderSummary>> summaries = outputTopic.readKeyValuesToList();
        
        assertEquals(1, summaries.size());
        
        // Verify order is processed under "UNKNOWN" key
        assertEquals("UNKNOWN", summaries.get(0).key);
        CustomerOrderSummary summary = summaries.get(0).value;
        assertEquals("UNKNOWN", summary.getCustomerId());
        assertEquals(150.0, summary.getTotalAmount());
        assertEquals(1, summary.getOrderCount());
        assertEquals(150.0, summary.getAvgOrderValue());
    }
}