# 📦 Kafka Assignment

A Spring Boot application demonstrating Kafka message production, real-time price aggregation, DLQ (Dead Letter Queue) handling, and REST API endpoints for managing order events.

---

## 🚀 Project Overview

This project simulates an **Order Processing System** using **Apache Kafka**.  
It includes:

- **Order Producer** – Publishes orders to Kafka  
- **Kafka Consumers** – Listens and processes incoming orders  
- **Real-Time Price Aggregation** – Computes running totals and averages  
- **Dead Letter Queue (DLQ)** – Collects invalid or failed messages  
- **REST API** – Endpoints for sending orders, retrieving stats, viewing DLQ data  
- **Health Check** – View system status  

---

## 🛠️ Technology Stack

| Component | Technology |
|----------|------------|
| Backend | Spring Boot |
| Messaging | Apache Kafka |
| Build Tool | Maven |
| Java | Java 17 |
| Logging | SLF4J |
| Lombok | For boilerplate reduction |

---


---

## 📡 Kafka Topics Used

| Topic | Purpose |
|-------|---------|
| `order-topic` | Main topic for processing orders |
| `order-topic.DLQ` | Dead Letter Queue for failed messages |

---

## ▶️ Running the Project

### **1. Start Kafka (Docker method)**

```bash
docker-compose up -d --build
```
