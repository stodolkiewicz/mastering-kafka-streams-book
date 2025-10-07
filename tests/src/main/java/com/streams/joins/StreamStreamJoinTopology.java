package com.streams.joins;

import com.streams.common.model.Order;
import com.streams.common.model.OrderWithPayment;
import com.streams.common.model.Payment;
import com.streams.common.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * Demonstrates Stream-Stream join with windowing in Kafka Streams.
 *
 * Stream-Stream joins combine two event streams based on:
 * - Common key (orderId in this case)
 * - Time window (events must occur within specified time range)
 * - Join semantics (inner, left, outer)
 *
 * Left Join behavior:
 * - Always emits when left stream (Order) event arrives
 * - Right stream (Payment) can be null if no matching event in window
 * - Only left stream arrival triggers output emission
 *
 * Business scenario: Join Order events with Payment events to track order fulfillment.
 * Uses asymmetric window to ensure Payment cannot come before Order (fraud prevention).
 */
public class StreamStreamJoinTopology {
    private static final Logger logger = LoggerFactory.getLogger(StreamStreamJoinTopology.class);

    public static Topology createStreamStreamJoinTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // STEP 1: Create order stream (LEFT stream - primary)
        KStream<String, Order> orderStream = builder.stream(
                "orders-topic",
                Consumed.with(
                        Serdes.String(),
                        new JsonSerDes<>(Order.class)
                )
        );

        // STEP 2: Create payment stream (RIGHT stream)
        // Filter out invalid payments and rekey by orderId for join
        KStream<String, Payment> paymentStream = builder.stream(
                "payments-topic",
                Consumed.with(
                        Serdes.String(),
                        new JsonSerDes<>(Payment.class)
                )
        )
        // Filter invalid payments before processing to avoid downstream issues
        .filter((paymentId, payment) -> {
            boolean isValid = payment != null && payment.getOrderId() != null;
            if (!isValid) {
                logger.warn("Filtering invalid payment: key={}, payment={}", paymentId, payment);
            }
            return isValid;
        })
        // Rekey by orderId to enable join with Order stream
        // After filter, payment.getOrderId() is guaranteed to be non-null
        .selectKey((paymentId, payment) -> payment.getOrderId());

        // STEP 3: Define asymmetric join window
        // Payment cannot be before Order (.before(0)), but can be up to 10 minutes after
        // This enforces business rule: Order must come before Payment (fraud prevention)
        JoinWindows joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10))
                .before(Duration.ofMinutes(0)); // No payments allowed before order


        // STEP 4: Define value joiner
        // This function is called when:
        // - Both Order and Payment are present in window (payment != null)
        // - Only Order is present and window closes (payment == null - Left Join behavior)
        ValueJoiner<Order, Payment, OrderWithPayment> valueJoiner = (order, payment) -> {
            // Determine join status based on payment status (null-safe)
            String joinStatus = Optional.ofNullable(payment)
                    .map(Payment::getStatus)
                    .map(status -> switch (status) {
                        case "SUCCESS" -> "PAYMENT_PAID";
                        case "FAILED" -> "PAYMENT_FAILED";
                        default -> "PENDING_PAYMENT"; // PROCESSING, PENDING, etc.
                    })
                    .orElse("PENDING_PAYMENT"); // payment is null (Left Join case)

            return new OrderWithPayment(order, payment, joinStatus);
        };

        // STEP 5: Perform LEFT JOIN
        // Left Join guarantees:
        // - Always emits when Order (left stream) arrives
        // - Payment (right stream) can be null if no match in window
        // - Only Order arrival triggers output (Payment arrival alone doesn't emit)
        // - Allows tracking of unpaid orders (business requirement)
        KStream<String, OrderWithPayment> joinedStream = orderStream.leftJoin(
                paymentStream,
                valueJoiner,
                joinWindow,
                StreamJoined.with(
                    Serdes.String(), // Key serde for join stores
                    new JsonSerDes<>(Order.class), // Left value serde
                    new JsonSerDes<>(Payment.class) // Right value serde
                )
        );

        // STEP 6: Debug logging
        // peek() allows observing stream without modifying it (side-effect only)
        joinedStream.peek((key, orderWithPayment) -> 
            logger.info("Joined result: orderId={}, joinStatus={}, hasPayment={}", 
                key, orderWithPayment.getJoinStatus(), orderWithPayment.getPayment() != null));

        // STEP 7: Write to output topic
        joinedStream.to("order-payment-results",
                Produced.with(Serdes.String(), new JsonSerDes<>(OrderWithPayment.class)));


        return builder.build();
    }
}