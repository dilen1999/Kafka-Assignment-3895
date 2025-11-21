package com.BigData.Assignment.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class PriceAggregationService {

    @Getter
    private final AtomicReference<Double> runningAverage = new AtomicReference<>(0.0);

    @Getter
    private final AtomicInteger totalOrders = new AtomicInteger(0);

    private final AtomicReference<Double> totalSum = new AtomicReference<>(0.0);

    @Getter
    private volatile double minPrice = Double.MAX_VALUE;

    @Getter
    private volatile double maxPrice = Double.MIN_VALUE;

    
    public synchronized void addPrice(float price) {
        int currentCount = totalOrders.get();
        double currentSum = totalSum.get();

        // Update sum and count
        double newSum = currentSum + price;
        int newCount = currentCount + 1;

        totalSum.set(newSum);
        totalOrders.set(newCount);

        // Calculate new running average
        double newAverage = newSum / newCount;
        runningAverage.set(newAverage);

        // Update min/max
        if (price < minPrice) {
            minPrice = price;
        }
        if (price > maxPrice) {
            maxPrice = price;
        }

        log.info("Price Aggregation Update - New Price: ${:.2f} | Running Avg: ${:.2f} | Total Orders: {} | Min: ${:.2f} | Max: ${:.2f}",
                price, newAverage, newCount, minPrice, maxPrice);
    }

    public AggregationStats getStats() {
        return new AggregationStats(
                runningAverage.get(),
                totalOrders.get(),
                minPrice == Double.MAX_VALUE ? 0.0 : minPrice,
                maxPrice == Double.MIN_VALUE ? 0.0 : maxPrice,
                totalSum.get()
        );
    }

    public void reset() {
        runningAverage.set(0.0);
        totalOrders.set(0);
        totalSum.set(0.0);
        minPrice = Double.MAX_VALUE;
        maxPrice = Double.MIN_VALUE;
        log.info("Price aggregation statistics reset");
    }

    public record AggregationStats(
            double runningAverage,
            int totalOrders,
            double minPrice,
            double maxPrice,
            double totalSum
    ) {}
}