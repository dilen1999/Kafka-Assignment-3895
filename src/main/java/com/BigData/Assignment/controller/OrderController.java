package com.BigData.Assignment.controller;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.BigData.Assignment.service.OrderProducerService;
import com.BigData.Assignment.service.PriceAggregationService;
import com.BigData.Assignment.service.DLQConsumerService;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderProducerService producerService;
    private final PriceAggregationService aggregationService;
    private final DLQConsumerService dlqConsumerService;


    @PostMapping("/sendorders")
    public ResponseEntity<Map<String, Object>> sendOrder(@RequestBody OrderRequest request) {
        try {
            // Validate input
            if (request.orderId() == null || request.orderId().trim().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "error",
                        "message", "order id is required"
                ));
            }
            if (request.product() == null || request.product().trim().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "error",
                        "message", "product name required"
                ));
            }
            if (request.price() <= 0) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "error",
                        "message", "price must be greater than zero"
                ));
            }

            log.info("REST API: Sending order - ID: {}, Product: {}, Price: {}", 
                    request.orderId(), request.product(), request.price());
            producerService.sendOrder(request.orderId(), request.product(), request.price());

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "order sent to Kafka",
                    "orderId", request.orderId(),
                    "product", request.product(),
                    "price", request.price()
            ));
        } catch (Exception e) {
            log.error("Error sending order", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "error",
                    "message", "Failed to send order: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/send-batch-orders")
    public ResponseEntity<Map<String, Object>> sendBatchOrders(@RequestBody List<OrderRequest> orders) {
        try {
            if (orders == null || orders.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "error",
                        "message", "order cannot be empty"
                ));
            }

            log.info("REST API: Sending batch of {} orders", orders.size());
            int successCount = 0;
            int failCount = 0;

            for (OrderRequest order : orders) {
                try {
                    if (order.orderId() != null && !order.orderId().trim().isEmpty() &&
                        order.product() != null && !order.product().trim().isEmpty() &&
                        order.price() > 0) {
                        producerService.sendOrder(order.orderId(), order.product(), order.price());
                        successCount++;
                    } else {
                        log.warn("Skipping invalid order: {}", order);
                        failCount++;
                    }
                } catch (Exception e) {
                    log.error("Failed to send order: {}", order.orderId(), e);
                    failCount++;
                }
            }

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", String.format("Batch processing complete"),
                    "totalOrders", orders.size(),
                    "successCount", successCount,
                    "failCount", failCount
            ));
        } catch (Exception e) {
            log.error("Error sending batch orders", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "error",
                    "message", "Failed to send batch orders: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/get-stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        log.info("Fetching aggregation stats");
        PriceAggregationService.AggregationStats stats = aggregationService.getStats();
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "stats", stats
        ));
    }

    @PostMapping("/stats/reset")
    public ResponseEntity<Map<String, String>> resetStats() {
        log.info(" Resetting aggregation stats");
        aggregationService.reset();

        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Aggregation statistics reset"
        ));
    }

    @GetMapping("/failed")
    public ResponseEntity<Map<String, Object>> getFailedOrders() {
        log.info("Fetching failed orders from DLQ");
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "failedOrderCount", dlqConsumerService.getFailedOrderCount(),
                "failedOrders", dlqConsumerService.getFailedOrders()
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Kafka Order System",
                "totalOrdersProcessed", aggregationService.getTotalOrders().get(),
                "currentAverage", aggregationService.getRunningAverage().get(),
                "failedOrders", dlqConsumerService.getFailedOrderCount()
        ));
    }

    // Request DTO
    public record OrderRequest(
            String orderId,
            String product,
            float price
    ) {}
}
