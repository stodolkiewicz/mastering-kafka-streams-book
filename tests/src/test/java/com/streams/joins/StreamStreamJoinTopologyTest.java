package com.streams.joins;

import com.streams.common.model.Order;
import com.streams.common.model.OrderWithPayment;
import com.streams.common.model.Payment;
import com.streams.common.serde.JsonDeserializer;
import com.streams.common.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests StreamStreamJoinTopology demonstrating windowed joins between Order and Payment streams.
 * 
 * Key scenarios tested:
 * - Successful joins within asymmetric window
 * - Left join behavior (Order without Payment)  
 * - Window boundary conditions
 * - Different payment statuses
 * - Invalid payment filtering
 */
class StreamStreamJoinTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> orderTopic;
    private TestInputTopic<String, Payment> paymentTopic;
    private TestOutputTopic<String, OrderWithPayment> outputTopic;

    @BeforeEach
    void setup() {
        Topology topology = StreamStreamJoinTopology.createStreamStreamJoinTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, props);

        orderTopic = testDriver.createInputTopic(
                "orders-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        paymentTopic = testDriver.createInputTopic(
                "payments-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "order-payment-results",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(OrderWithPayment.class)
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldJoinOrderWithSuccessfulPayment() {
        // given
        Order order = new Order("order-1", "customer-1", 100.0, "PENDING");
        Payment payment = new Payment("payment-1", "order-1", 100.0, "SUCCESS", "CARD");
        
        Instant orderTime = Instant.now();
        Instant paymentTime = orderTime.plus(5, ChronoUnit.MINUTES);

        // when
        orderTopic.pipeInput("order-1", order, orderTime);
        paymentTopic.pipeInput("order-1", payment, paymentTime);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        
        List<OrderWithPayment> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        
        OrderWithPayment result = results.get(0);
        assertEquals("order-1", result.getOrder().getOrderId());
        assertNotNull(result.getPayment());
        assertEquals("payment-1", result.getPayment().getPaymentId());
        assertEquals("PAYMENT_PAID", result.getJoinStatus());
    }

    @Test
    void shouldJoinOrderWithFailedPayment() {
        // given
        Order order = new Order("order-2", "customer-2", 200.0, "PENDING");
        Payment payment = new Payment("payment-2", "order-2", 200.0, "FAILED", "CARD");
        
        Instant orderTime = Instant.now();
        Instant paymentTime = orderTime.plus(2, ChronoUnit.MINUTES);

        // when
        orderTopic.pipeInput("order-2", order, orderTime);
        paymentTopic.pipeInput("order-2", payment, paymentTime);

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        
        OrderWithPayment result = results.get(0);
        assertEquals("PAYMENT_FAILED", result.getJoinStatus());
        assertEquals("FAILED", result.getPayment().getStatus());
    }

    @Test
    void shouldJoinOrderWithPendingPayment() {
        // given
        Order order = new Order("order-3", "customer-3", 150.0, "PENDING");
        Payment payment = new Payment("payment-3", "order-3", 150.0, "PROCESSING", "TRANSFER");
        
        Instant orderTime = Instant.now();
        Instant paymentTime = orderTime.plus(1, ChronoUnit.MINUTES);

        // when
        orderTopic.pipeInput("order-3", order, orderTime);
        paymentTopic.pipeInput("order-3", payment, paymentTime);

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        
        OrderWithPayment result = results.get(0);
        assertEquals("PENDING_PAYMENT", result.getJoinStatus());
        assertEquals("PROCESSING", result.getPayment().getStatus());
    }

    @Test
    void shouldEmitOrderWithoutPayment_LeftJoinBehavior() {
        // given
        Order order = new Order("order-4", "customer-4", 75.0, "PENDING");
        Instant orderTime = Instant.now();

        // when
        orderTopic.pipeInput("order-4", order, orderTime);
        
        // Force stream time advancement beyond window (spurious results fix requires this)
        Order dummyOrder = new Order("DUMMY", "DUMMY", 0.0, "DUMMY");
        orderTopic.pipeInput("DUMMY", dummyOrder, orderTime.plus(15, ChronoUnit.MINUTES));

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList()
            .stream()
            .filter(r -> !"DUMMY".equals(r.getOrder().getOrderId()))
            .toList();
        
        assertThat(results).hasSize(1);
        
        OrderWithPayment result = results.get(0);
        assertEquals("order-4", result.getOrder().getOrderId());
        assertNull(result.getPayment());
        assertEquals("PENDING_PAYMENT", result.getJoinStatus());
    }

    @Test
    void shouldNotJoinWhenPaymentOutsideWindow() {
        // given
        Order order = new Order("order-5", "customer-5", 300.0, "PENDING");
        Payment payment = new Payment("payment-5", "order-5", 300.0, "SUCCESS", "CARD");
        
        Instant orderTime = Instant.now();
        Instant paymentTime = orderTime.plus(15, ChronoUnit.MINUTES); // Beyond 10-minute window

        // when
        orderTopic.pipeInput("order-5", order, orderTime);
        paymentTopic.pipeInput("order-5", payment, paymentTime);
        
        // Force stream time advancement beyond payment time (to close all windows)
        Order dummyOrder = new Order("DUMMY", "DUMMY", 0.0, "DUMMY");
        orderTopic.pipeInput("DUMMY", dummyOrder, paymentTime.plus(5, ChronoUnit.MINUTES));

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList()
            .stream()
            .filter(r -> !"DUMMY".equals(r.getOrder().getOrderId()))
            .toList();
        
        assertThat(results).hasSize(1); // Only left join result
        
        OrderWithPayment result = results.get(0);
        assertEquals("PENDING_PAYMENT", result.getJoinStatus());
        assertNull(result.getPayment()); // Payment was outside window
    }

    @Test
    void shouldNotJoinWhenPaymentBeforeOrder_AsymmetricWindow() {
        // given
        Order order = new Order("order-6", "customer-6", 400.0, "PENDING");
        Payment payment = new Payment("payment-6", "order-6", 400.0, "SUCCESS", "CARD");
        
        Instant orderTime = Instant.now();
        Instant paymentTime = orderTime.minus(5, ChronoUnit.MINUTES); // Payment 5 min before Order

        // when
        paymentTopic.pipeInput("order-6", payment, paymentTime);
        orderTopic.pipeInput("order-6", order, orderTime);
        
        // Force stream time advancement beyond order window
        Order dummyOrder = new Order("DUMMY", "DUMMY", 0.0, "DUMMY");
        orderTopic.pipeInput("DUMMY", dummyOrder, orderTime.plus(15, ChronoUnit.MINUTES));

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList()
            .stream()
            .filter(r -> !"DUMMY".equals(r.getOrder().getOrderId()))
            .toList();
        
        assertThat(results).hasSize(1); // Only left join result
        
        OrderWithPayment result = results.get(0);
        assertEquals("PENDING_PAYMENT", result.getJoinStatus());
        assertNull(result.getPayment()); // Payment was before Order (asymmetric window)
    }

    @Test
    void shouldFilterInvalidPayments() {
        // given
        Order order = new Order("order-7", "customer-7", 500.0, "PENDING");
        Payment validPayment = new Payment("payment-7a", "order-7", 500.0, "SUCCESS", "CARD");
        Payment invalidPayment = new Payment("payment-7b", null, 100.0, "SUCCESS", "CARD"); // null orderId
        
        Instant orderTime = Instant.now();
        Instant validPaymentTime = orderTime.plus(2, ChronoUnit.MINUTES);
        Instant invalidPaymentTime = orderTime.plus(3, ChronoUnit.MINUTES);

        // when
        orderTopic.pipeInput("order-7", order, orderTime);
        paymentTopic.pipeInput("order-7", validPayment, validPaymentTime);
        paymentTopic.pipeInput("invalid-key", invalidPayment, invalidPaymentTime); // Should be filtered

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1); // Only valid payment joined
        
        OrderWithPayment result = results.get(0);
        assertEquals("PAYMENT_PAID", result.getJoinStatus());
        assertEquals("payment-7a", result.getPayment().getPaymentId());
    }

    @Test
    void shouldHandleMultipleOrdersAndPayments() {
        // given
        Order order1 = new Order("order-A", "customer-A", 100.0, "PENDING");
        Order order2 = new Order("order-B", "customer-B", 200.0, "PENDING");
        Payment payment1 = new Payment("payment-A", "order-A", 100.0, "SUCCESS", "CARD");
        Payment payment2 = new Payment("payment-B", "order-B", 200.0, "FAILED", "TRANSFER");
        
        Instant baseTime = Instant.now();

        // when
        orderTopic.pipeInput("order-A", order1, baseTime);
        orderTopic.pipeInput("order-B", order2, baseTime.plus(30, ChronoUnit.SECONDS));
        paymentTopic.pipeInput("order-A", payment1, baseTime.plus(1, ChronoUnit.MINUTES));
        paymentTopic.pipeInput("order-B", payment2, baseTime.plus(2, ChronoUnit.MINUTES));

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(2);
        
        // Find results by order ID
        OrderWithPayment resultA = results.stream()
                .filter(r -> "order-A".equals(r.getOrder().getOrderId()))
                .findFirst()
                .orElseThrow();
        
        OrderWithPayment resultB = results.stream()
                .filter(r -> "order-B".equals(r.getOrder().getOrderId()))
                .findFirst()
                .orElseThrow();
        
        assertEquals("PAYMENT_PAID", resultA.getJoinStatus());
        assertEquals("PAYMENT_FAILED", resultB.getJoinStatus());
    }

    @Test
    void shouldJoinAtWindowBoundary() {
        // given
        Order order = new Order("order-8", "customer-8", 250.0, "PENDING");
        Payment payment = new Payment("payment-8", "order-8", 250.0, "SUCCESS", "CARD");
        
        Instant orderTime = Instant.now();
        Instant paymentTime = orderTime.plus(10, ChronoUnit.MINUTES); // Exactly at window boundary

        // when
        orderTopic.pipeInput("order-8", order, orderTime);
        paymentTopic.pipeInput("order-8", payment, paymentTime);

        // then
        List<OrderWithPayment> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        
        OrderWithPayment result = results.get(0);
        assertEquals("PAYMENT_PAID", result.getJoinStatus());
        assertNotNull(result.getPayment());
    }
}