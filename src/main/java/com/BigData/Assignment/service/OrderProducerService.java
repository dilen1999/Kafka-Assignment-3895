package com.BigData.Assignment.service;

import com.BigData.Assignment.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    public void sendOrder(String orderId, String product, float price) {
        Order order = Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(ordersTopic, orderId, order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent successfully: {} - Topic: {} - Partition: {} - Offset: {}",
                        orderId,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order: {}", orderId, ex);
            }
        });
    }
}