package com.BigData.Assignment.service;

import com.BigData.Assignment.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderConsumerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PriceAggregationService aggregationService;

    @Value("${kafka.topics.orders-dlq}")
    private String dlqTopic;

    @Value("${kafka.topics.orders-retry}")
    private String retryTopic;

    @Value("${kafka.consumer.max-retry-attempts}")
    private int maxRetryAttempts;

    @Value("${kafka.consumer.retry-delay-ms}")
    private long retryDelayMs;

    @KafkaListener(topics = "${kafka.topics.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrder(@Payload Order order,
                             @Header(KafkaHeaders.RECEIVED_KEY) String key,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset,
                             Acknowledgment acknowledgment) {
        try {
            log.info("Received order: {} - Product: {} - Price: ${} - Partition: {} - Offset: {}",
                    order.getOrderId().toString(), order.getProduct().toString(), order.getPrice(), partition, offset);

            
            processOrder(order);

            
            aggregationService.addPrice(order.getPrice());

            
            acknowledgment.acknowledge();

            log.info("Order processed successfully: {}", order.getOrderId().toString());

        } catch (Exception e) {
            log.error("Error processing order: {} - Attempting retry", order.getOrderId().toString(), e);
            handleFailure(order, 0, acknowledgment);
        }
    }

    @KafkaListener(topics = "${kafka.topics.orders-retry}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeRetryOrder(@Payload Order order,
                                  @Header(value = "retry-count", defaultValue = "0") int retryCount,
                                  Acknowledgment acknowledgment) {
        try {
            log.info("Retry attempt {} for order: {}", retryCount, order.getOrderId());

            // Simulate retry delay
            Thread.sleep(retryDelayMs);

            // Process the order
            processOrder(order);

            // Update aggregation
            aggregationService.addPrice(order.getPrice());

            acknowledgment.acknowledge();
            log.info("Order processed successfully after {} retries: {}", retryCount, order.getOrderId());

        } catch (Exception e) {
            log.error("Error on retry {} for order: {}", retryCount, order.getOrderId(), e);
            handleFailure(order, retryCount, acknowledgment);
        }
    }

    private void processOrder(Order order) throws Exception {
       
        log.debug("Processing order: {} - Product: {} - Price: ${}",
                order.getOrderId(), order.getProduct(), order.getPrice());

        
        if (order.getOrderId() == null || order.getOrderId().toString().isEmpty()) {
            throw new IllegalArgumentException("Order ID cannot be empty");
        }
        if (order.getProduct() == null || order.getProduct().toString().isEmpty()) {
            throw new IllegalArgumentException("Product name cannot be empty");
        }
        if (order.getPrice() <= 0) {
            throw new IllegalArgumentException("Price must be greater than zero");
        }

        
        log.info("Order validated and processed: {}", order.getOrderId());
    }

    private void handleFailure(Order order, int retryCount, Acknowledgment acknowledgment) {
        retryCount++;
        final int finalRetryCount = retryCount;

        if (retryCount < maxRetryAttempts) {
            
            log.warn("Sending order {} to retry topic (attempt {})", order.getOrderId(), retryCount);
            kafkaTemplate.send(retryTopic, order.getOrderId().toString(), order)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send to retry topic: {}", order.getOrderId(), ex);
                            sendToDLQ(order, finalRetryCount, "Failed to send to retry topic: " + ex.getMessage());
                        }
                    });
        } else {
            
            log.error("Max retry attempts exceeded for order: {}. Sending to DLQ", order.getOrderId());
            sendToDLQ(order, retryCount, "Max retry attempts exceeded");
        }

        
        acknowledgment.acknowledge();
    }

    private void sendToDLQ(Order order, int retryCount, String reason) {
        log.error("Sending order {} to DLQ. Reason: {} - Retry count: {}",
                order.getOrderId(), reason, retryCount);

        kafkaTemplate.send(dlqTopic, order.getOrderId().toString(), order)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Order sent to DLQ successfully: {}", order.getOrderId());
                    } else {
                        log.error("Failed to send order to DLQ: {}", order.getOrderId(), ex);
                    }
                });
    }
}