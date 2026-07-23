# 🏦 Digital Banking System - Microservices Architecture

A production-inspired **Digital Banking System** built using **Spring Boot Microservices**, **Apache Kafka**, **Redis**, **Spring Cloud**, and **MySQL**.

This project demonstrates how modern banking applications can be built using an event-driven architecture with distributed transactions, asynchronous communication, fraud detection, payment gateway integration, and secure service-to-service communication.

---

# ✨ Features

## 👤 Account Service
- Create Bank Account
- Get Account Details
- Credit Balance
- Debit Balance
- Block/Unblock Account
- Balance Validation

---

## 💸 Transaction Service
- Bank-to-Bank Money Transfer
- OTP Verification
- Transaction History
- Transaction Status Tracking
- Distributed Transaction using SAGA Pattern
- Automatic Compensation (Refund)

---

## 🚨 Fraud Detection Service
- Velocity Fraud Detection
- Suspicious Amount Detection
- Redis Based Fraud Rules
- Publish Fraud Events
- Automatic Account Blocking

---

## 💳 Payment Service
- Razorpay Order Creation
- Secure Webhook Handling
- Payment Status Tracking
- UPI/Card/NetBanking Payments
- Event Driven Payment Completion

---

## 🔔 Notification Service
- Transaction Alerts
- Payment Success Notifications
- Payment Failure Notifications
- Refund Notifications

---

## 🌐 API Gateway
- Centralized Routing
- Rate Limiting (Redis)
- Single Entry Point
- Gateway Filters

---

# 🛠 Tech Stack

### Backend

- Java
- Spring Boot
- Spring Data JPA
- Spring Validation
- Spring Cloud Gateway
- Spring Cloud OpenFeign

### Messaging

- Apache Kafka

### Database

- MySQL

### Cache

- Redis

### Payment Gateway

- Razorpay

### Build Tool

- Maven

---

# 🧩 Microservices

```
banking-app
│
├── account-service
│
├── transaction-service
│
├── fraud-detection-service
│
├── payment-service
│
├── notification-service
│
└── api-gateway
```

---

# 🏗 Architecture

```
                    Client
                       │
                       ▼
               API Gateway
                       │
     ┌─────────────────┼──────────────────┐
     ▼                 ▼                  ▼
Account Service   Transaction Service   Payment Service
     │                 │                  │
     │                 │                  │
     ▼                 ▼                  ▼
  MySQL            Apache Kafka      Razorpay API
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
 Fraud Detection Service    Notification Service
          │
          ▼
        Redis
```

---

# 🔄 Bank Transfer Flow

```
Sender
   │
   ▼
Transaction Service
   │
Deduct Sender Balance
   │
Save Transaction (PROCESSING)
   │
Publish transaction.initiated
   │
Fraud Detection Service
   │
────────────────────────────────────
│ Fraud Found ?
────────────────────────────────────
      │                │
     YES              NO
      │                │
Refund Sender      Generate OTP
Block Account          │
Notify User            │
                       ▼
               OTP Verification
                       │
               Publish transaction.completed
                       │
                Account Service
                       │
             Credit Receiver Account
                       │
               Notification Service
```

---

# 💳 Payment Flow (Razorpay)

```
Frontend
     │
     ▼
Payment Service
     │
Create Razorpay Order
     │
Frontend Checkout
     │
Customer Pays
     │
Razorpay Webhook
     │
───────────────────────────────
│ Payment Success ?
───────────────────────────────
      │              │
     YES            NO
      │              │
Publish           Publish
payment.completed payment.failed
      │              │
      ▼              ▼
Account Service   Notification Service
      │
Credit Receiver
```

---

# ⚡ Kafka Topics

| Topic | Producer | Consumer |
|--------|----------|----------|
| transaction.initiated | Transaction Service | Fraud Detection Service |
| transaction.completed | Transaction Service | Account Service, Notification Service |
| fraud.detected | Fraud Detection Service | Notification Service |
| transaction.refunded | Transaction Service | Notification Service |
| payment.completed | Payment Service | Account Service, Notification Service |
| payment.failed | Payment Service | Notification Service |

---

# 📦 Services Used

- Spring Boot
- Spring Data JPA
- Spring Cloud OpenFeign
- Spring Cloud Gateway
- Apache Kafka
- Redis
- MySQL
- Razorpay
- Maven

---

# 🔐 Security Features

- OTP Verification
- Fraud Detection
- Account Blocking
- Distributed Transactions
- Automatic Compensation
- Event Driven Communication
- Rate Limiting

---

# 📚 Concepts Demonstrated

- Microservices Architecture
- Event Driven Architecture
- Apache Kafka
- Producer & Consumer
- Spring Cloud OpenFeign
- API Gateway
- Distributed Transactions
- SAGA Pattern
- Redis
- Caching
- Webhooks
- REST APIs
- MySQL
- Spring Validation
- Service Communication

---


---

# 🚀 Getting Started

## Clone Repository

```bash
git clone https://github.com/Saurav3004/Banking-System-with-fraud-detection
```

```
cd Banking-System-with-fraud-detection
```

---

## Start Infrastructure

- MySQL
- Apache Kafka
- Zookeeper
- Redis

---

## Run Services

Start services in the following order:

1. Account Service
2. Transaction Service
3. Fraud Detection Service
4. Notification Service
5. Payment Service
6. API Gateway

---

# Future Improvements

- JWT Authentication
- Docker & Docker Compose
- Kubernetes Deployment
- Service Discovery (Eureka)
- Circuit Breaker (Resilience4j)
- Distributed Tracing
- Prometheus & Grafana
- ELK Logging
- CI/CD Pipeline
- RabbitMQ Support

---

# Author

**Saurav Jha**

Full Stack Engineer | MERN | Java | Spring Boot | Microservices | Apache Kafka | Redis | MySQL

LinkedIn: https://www.linkedin.com/in/saurav-jha-41431b255/

GitHub: https://github.com/Saurav3004

---

If you found this project interesting, consider giving it a ⭐ on GitHub!