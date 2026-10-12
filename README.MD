# Kafka Examples (Simple Kafka)

A minimal Spring Boot project demonstrating the **plain Spring for Apache Kafka** (`spring-kafka`) producer-consumer flow. This branch focuses on the fundamental building blocks of Kafka integration in Spring Boot without any higher-level abstraction like Spring Cloud Stream.

## Overview

This repository contains two independent Spring Boot applications:

- **Producer** – Sends a simple string message to a Kafka topic on application startup using `KafkaTemplate`.
- **Consumer** – Listens on the same topic and logs the received message to the console using `@KafkaListener`.

The project uses the `spring-kafka` starter directly, which provides the `KafkaTemplate` for sending messages and the `@KafkaListener` annotation for consuming them. The producer sends its first message via a `CommandLineRunner` bean, making it easy to observe the end-to-end flow immediately after startup[reference:0].

## Prerequisites

- Java 17 or later
- Maven 3.9+
- Docker and Docker Compose (for running Kafka)

## Getting Started

### 1. Start Kafka

A `docker-compose.yaml` file is provided to spin up a Kafka instance in KRaft mode along with Kafka UI for easy topic inspection[reference:1].

```bash
docker compose up -d
```

Kafka UI will be available at http://localhost:8090, and the Kafka broker will be accessible at `localhost:9092`[reference:2].

### 2. Run the Producer

Navigate to the `producer` directory and start the application:

```bash
cd producer
./mvnw spring-boot:run
```

The producer runs on port `8081` and sends a single message to the `my-topic` topic on startup[reference:3][reference:4].

### 3. Run the Consumer

In a separate terminal, navigate to the `consumer` directory and start the application:

```bash
cd consumer
./mvnw spring-boot:run
```

The consumer runs on port `8082` and listens on the `my-topic` topic with the consumer group `my-consumer-group`[reference:5]. Received messages are logged to the console[reference:6].

## Configuration

Both applications use the `spring-kafka` starter and connect to the Kafka broker at `localhost:9092`.

### Producer Configuration (`producer/src/main/resources/application.yaml`)

```yaml
server:
port: 8081

spring:
application:
name: producer
kafka:
bootstrap-servers: localhost:9092
producer:
key-serializer: org.apache.kafka.common.serialization.StringSerializer
value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
admin:
auto-create: true
```

The producer uses `StringSerializer` for keys and `JacksonJsonSerializer` for values, and enables automatic topic creation via `spring.kafka.admin.auto-create: true`[reference:7].

### Consumer Configuration (`consumer/src/main/resources/application.yaml`)

```yaml
server:
port: 8082

spring:
application:
name: consumer
kafka:
bootstrap-servers: localhost:9092
consumer:
group-id: my-consumer-group
auto-offset-reset: earliest
key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
properties:
spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
spring.json.trusted.packages: "com.example.*,java.util,java.lang"
```

The consumer uses `ErrorHandlingDeserializer` to gracefully handle deserialization failures and sets `auto-offset-reset: earliest` so it reads from the beginning of the topic on first startup[reference:8].

### Key Configuration Properties

| Property | Description |
|----------|-------------|
| `spring.kafka.bootstrap-servers` | Kafka broker address(es) |
| `spring.kafka.producer.key-serializer` | Serializer for message keys |
| `spring.kafka.producer.value-serializer` | Serializer for message values |
| `spring.kafka.admin.auto-create` | Enable automatic topic creation |
| `spring.kafka.consumer.group-id` | Consumer group name |
| `spring.kafka.consumer.auto-offset-reset` | Offset reset strategy (`earliest` or `latest`) |
| `spring.kafka.consumer.key-deserializer` | Deserializer for message keys |
| `spring.kafka.consumer.value-deserializer` | Deserializer for message values |

## Project Structure

```
kafka-examples/
├── docker-compose.yaml          # Kafka + Kafka UI services
├── consumer/                    # Spring Boot consumer app
│   ├── src/
│   │   └── main/
│   │       ├── java/com/example/consumer/
│   │       │   ├── ConsumerApplication.java
│   │       │   └── consumer/MyConsumer.java
│   │       └── resources/application.yaml
│   ├── pom.xml
│   └── mvnw
├── producer/                    # Spring Boot producer app
│   ├── src/
│   │   └── main/
│   │       ├── java/com/example/producer/
│   │       │   ├── ProducerApplication.java
│   │       │   └── services/MyProducerService.java
│   │       └── resources/application.yaml
│   ├── pom.xml
│   └── mvnw
└── README.MD
```

## Key Concepts Demonstrated

- **Plain Spring Kafka**: Using `KafkaTemplate` and `@KafkaListener` directly without any abstraction layer.
- **CommandLineRunner for Initial Send**: Sending a message immediately after application startup to demonstrate the end-to-end flow[reference:9].
- **Jackson JSON Serialization**: Configuring `JacksonJsonSerializer` and `JacksonJsonDeserializer` for structured message payloads[reference:10].
- **Error Handling Deserializer**: Using `ErrorHandlingDeserializer` to prevent consumer crashes on malformed messages[reference:11].
- **KRaft Mode Kafka**: Running Kafka without ZooKeeper using the Confluent `cp-kafka` image in KRaft mode[reference:12].

## Monitoring

Kafka UI is available at http://localhost:8090. You can use it to:

- Inspect the `my-topic` topic and its partitions.
- View message throughput and consumer group lag.
- Browse individual messages as they are produced.

## License

This project is provided for educational purposes. Feel free to use it as a starting point for your own Spring Kafka experiments.

---

# مثالهای Kafka (Kafka ساده)

یک پروژه مینیمال Spring Boot که جریان اصلی Producer–Consumer را با استفاده از **Spring for Apache Kafka** ساده (`spring-kafka`) نشان میدهد. این شاخه بر روی بلوکهای سازنده اصلی یکپارچهسازی Kafka در Spring Boot بدون هیچ لایه انتزاعی سطح بالاتری مانند Spring Cloud Stream تمرکز دارد.

## نمای کلی

این مخزن شامل دو اپلیکیشن مستقل Spring Boot است:

- **Producer (تولیدکننده)** – یک پیام رشتهای ساده را در زمان راهاندازی اپلیکیشن با استفاده از `KafkaTemplate` به یک تاپیک Kafka ارسال میکند.
- **Consumer (مصرفکننده)** – به همان تاپیک گوش میدهد و پیام دریافتی را با استفاده از `@KafkaListener` در کنسول لاگ میکند.

این پروژه مستقیماً از استارتر `spring-kafka` استفاده میکند که `KafkaTemplate` را برای ارسال پیامها و انوتیشن `@KafkaListener` را برای مصرف آنها فراهم میکند. Producer اولین پیام خود را از طریق یک bean `CommandLineRunner` ارسال میکند، که مشاهده جریان سرتاسری را بلافاصله پس از راهاندازی آسان میسازد[reference:13].

## پیشنیازها

- جاوا ۱۷ یا بالاتر
- Maven 3.9+
- Docker و Docker Compose (برای اجرای Kafka)

## شروع به کار

### ۱. راهاندازی Kafka

فایل `docker-compose.yaml` برای راهاندازی یک نمونه Kafka در حالت KRaft همراه با Kafka UI برای بررسی آسان تاپیکها فراهم شده است[reference:14].

```bash
docker compose up -d
```

Kafka UI در آدرس http://localhost:8090 در دسترس خواهد بود و کارگزار Kafka از طریق `localhost:9092` قابل دسترسی است[reference:15].

### ۲. اجرای Producer

به پوشه `producer` بروید و اپلیکیشن را اجرا کنید:

```bash
cd producer
./mvnw spring-boot:run
```

Producer روی پورت `8081` اجرا میشود و یک پیام واحد را در زمان راهاندازی به تاپیک `my-topic` ارسال میکند[reference:16][reference:17].

### ۳. اجرای Consumer

در یک ترمینال جداگانه، به پوشه `consumer` بروید و اپلیکیشن را اجرا کنید:

```bash
cd consumer
./mvnw spring-boot:run
```

Consumer روی پورت `8082` اجرا میشود و به تاپیک `my-topic` با گروه مصرفکننده `my-consumer-group` گوش میدهد[reference:18]. پیامهای دریافتی در کنسول لاگ میشوند[reference:19].

## تنظیمات

هر دو اپلیکیشن از استارتر `spring-kafka` استفاده میکنند و به کارگزار Kafka در `localhost:9092` متصل میشوند.

### تنظیمات Producer (`producer/src/main/resources/application.yaml`)

```yaml
server:
port: 8081

spring:
application:
name: producer
kafka:
bootstrap-servers: localhost:9092
producer:
key-serializer: org.apache.kafka.common.serialization.StringSerializer
value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
admin:
auto-create: true
```

Producer از `StringSerializer` برای کلیدها و `JacksonJsonSerializer` برای مقادیر استفاده میکند و ایجاد خودکار تاپیک را از طریق `spring.kafka.admin.auto-create: true` فعال میکند[reference:20].

### تنظیمات Consumer (`consumer/src/main/resources/application.yaml`)

```yaml
server:
port: 8082

spring:
application:
name: consumer
kafka:
bootstrap-servers: localhost:9092
consumer:
group-id: my-consumer-group
auto-offset-reset: earliest
key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
properties:
spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
spring.json.trusted.packages: "com.example.*,java.util,java.lang"
```

Consumer از `ErrorHandlingDeserializer` برای مدیریت صحیح خطاهای deserialization استفاده میکند و `auto-offset-reset: earliest` را تنظیم میکند تا در اولین راهاندازی از ابتدای تاپیک بخواند[reference:21].

### خواص کلیدی تنظیمات

| خاصیت | توضیح |
|-------|-------|
| `spring.kafka.bootstrap-servers` | آدرس کارگزار(های) Kafka |
| `spring.kafka.producer.key-serializer` | سریالایزر برای کلیدهای پیام |
| `spring.kafka.producer.value-serializer` | سریالایزر برای مقادیر پیام |
| `spring.kafka.admin.auto-create` | فعالسازی ایجاد خودکار تاپیک |
| `spring.kafka.consumer.group-id` | نام گروه مصرفکننده |
| `spring.kafka.consumer.auto-offset-reset` | استراتژی بازنشانی offset (`earliest` یا `latest`) |
| `spring.kafka.consumer.key-deserializer` | دیسریالایزر برای کلیدهای پیام |
| `spring.kafka.consumer.value-deserializer` | دیسریالایزر برای مقادیر پیام |

## ساختار پروژه

```
kafka-examples/
├── docker-compose.yaml          # سرویسهای Kafka + Kafka UI
├── consumer/                    # اپلیکیشن مصرفکننده Spring Boot
│   ├── src/
│   │   └── main/
│   │       ├── java/com/example/consumer/
│   │       │   ├── ConsumerApplication.java
│   │       │   └── consumer/MyConsumer.java
│   │       └── resources/application.yaml
│   ├── pom.xml
│   └── mvnw
├── producer/                    # اپلیکیشن تولیدکننده Spring Boot
│   ├── src/
│   │   └── main/
│   │       ├── java/com/example/producer/
│   │       │   ├── ProducerApplication.java
│   │       │   └── services/MyProducerService.java
│   │       └── resources/application.yaml
│   ├── pom.xml
│   └── mvnw
└── README.MD
```

## مفاهیم کلیدی نشان داده شده

- **Spring Kafka ساده**: استفاده مستقیم از `KafkaTemplate` و `@KafkaListener` بدون هیچ لایه انتزاعی.
- **استفاده از CommandLineRunner برای ارسال اولیه**: ارسال پیام بلافاصله پس از راهاندازی اپلیکیشن برای نمایش جریان سرتاسری[reference:22].
- **سریالایزیشن JSON با Jackson**: پیکربندی `JacksonJsonSerializer` و `JacksonJsonDeserializer` برای بارهای پیام ساختاریافته[reference:23].
- **دیسریالایزر مدیریت خطا**: استفاده از `ErrorHandlingDeserializer` برای جلوگیری از خرابی Consumer در مواجهه با پیامهای نامعتبر[reference:24].
- **Kafka در حالت KRaft**: اجرای Kafka بدون ZooKeeper با استفاده از ایمیج Confluent `cp-kafka` در حالت KRaft[reference:25].

## پایش

Kafka UI در آدرس http://localhost:8090 در دسترس است. میتوانید از آن برای موارد زیر استفاده کنید:

- بررسی تاپیک `my-topic` و پارتیشنهای آن.
- مشاهده توان عملیاتی پیامها و تأخیر گروه مصرفکننده.
- مرور پیامهای تکتک هنگام تولید.

## مجوز

این پروژه برای اهداف آموزشی ارائه شده است. استفاده از آن به عنوان نقطه شروع برای آزمایشهای Spring Kafka خودتان آزاد است.