package com.BigData.Assignment.service;

import com.BigData.Assignment.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class DLQConsumerService {

    private final List<FailedOrder> failedOrders = Collections.synchronizedList(new ArrayList<>());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @KafkaListener(topics = "${kafka.topics.orders-dlq}", groupId = "${spring.kafka.consumer.group-id}-dlq")
    public void consumeDLQMessage(@Payload Order order,
                                  @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Header(KafkaHeaders.OFFSET) long offset,
                                  Acknowledgment acknowledgment) {

        String timestamp = LocalDateTime.now().format(formatter);

        log.error("===== DLQ MESSAGE RECEIVED =====");
        log.error("Timestamp: {}", timestamp);
        log.error("Order ID: {}", order.getOrderId());
        log.error("Product: {}", order.getProduct());
        log.error("Price: ${}", order.getPrice());
        log.error("Partition: {}", partition);
        log.error("Offset: {}", offset);
        log.error("================================");

        // Store failed order for monitoring
        failedOrders.add(new FailedOrder(
                order.getOrderId().toString(),
                order.getProduct().toString(),
                order.getPrice(),
                timestamp,
                partition,
                offset
        ));

        

        acknowledgment.acknowledge();
    }

    public List<FailedOrder> getFailedOrders() {
        return new ArrayList<>(failedOrders);
    }

    public int getFailedOrderCount() {
        return failedOrders.size();
    }

    public void clearFailedOrders() {
        failedOrders.clear();
        log.info("Cleared all failed orders from DLQ tracking");
    }

    public record FailedOrder(
            String orderId,
            String product,
            float price,
            String timestamp,
            int partition,
            long offset
    ) {}
}