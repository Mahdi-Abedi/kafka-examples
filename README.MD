# Kafka Examples (Spring Cloud Stream)

A minimal Spring Boot project demonstrating asynchronous messaging between two services using **Spring Cloud Stream** and **Apache Kafka** as the message broker. The application uses the functional programming model with `Supplier` and `Consumer` beans to decouple business logic from the messaging infrastructure.

## Overview

This repository contains two independent Spring Boot applications:

- **Producer** – Publishes messages to a Kafka topic via Spring Cloud Stream using a functional `Supplier` bean.
- **Consumer** – Listens on the same Kafka topic and processes incoming messages using a `Consumer` bean.

Spring Cloud Stream abstracts the messaging middleware, so the application code focuses on business logic rather than the underlying Kafka APIs. The `spring-cloud-stream-binder-kafka` dependency connects the framework to the broker. This branch demonstrates the core producer–consumer flow using Spring Cloud Stream's functional programming style.

## Prerequisites

- Java 17 or later
- Maven 3.9+
- Docker and Docker Compose (for running Kafka)

## Getting Started

### 1. Start Kafka

A `docker-compose.yaml` file is provided to spin up a Kafka instance along with Kafka UI for easy topic inspection.

```bash
docker compose up -d
```

Kafka UI will be available at http://localhost:8090, and the Kafka broker will be accessible at `localhost:9092`.

### 2. Run the Producer

Navigate to the `producer` directory and start the application:

```bash
cd producer
./mvnw spring-boot:run
```

The producer uses a functional `Supplier` bean to publish messages to a Spring Cloud Stream output binding. By default, the poller invokes the `Supplier` every 5 seconds. This can be adjusted via the `spring.integration.poller.fixed-delay` property.

### 3. Run the Consumer

In a separate terminal, navigate to the `consumer` directory and start the application:

```bash
cd consumer
./mvnw spring-boot:run
```

The consumer listens on the corresponding input binding. Received messages are logged to the console via a `Consumer` bean. The consumer processes messages as they arrive; if the producer emits faster than the consumer can process, messages will accumulate in the Kafka topic.

## Configuration

Both applications use Spring Cloud Stream's functional programming model with binding names derived from the bean name (`myMessageCreated`). The Kafka broker address defaults to `localhost:9092`.

### Producer Configuration (`producer/src/main/resources/application.yaml`)

```yaml
server:
port: 8081

spring:
application:
name: producer
cloud:
function:
definition: myMessageCreated
stream:
bindings:
myMessageCreated-out-0:
destination: my-topic
content-type: application/json
kafka:
binder:
brokers: localhost:9092
integration:
poller:
fixed-delay: 5000
```

The `spring.integration.poller.fixed-delay` property controls how often the `Supplier` is invoked. Change `5000` to any millisecond value to adjust the message frequency.

### Consumer Configuration (`consumer/src/main/resources/application.yaml`)

```yaml
server:
port: 8082

spring:
application:
name: consumer
cloud:
function:
definition: myMessageCreated
stream:
bindings:
myMessageCreated-in-0:
destination: my-topic
group: notification-group
kafka:
binder:
brokers: localhost:9092
```

The `group` property ensures the consumer belongs to a specific consumer group (`notification-group`), enabling scalable message consumption.

### Key Configuration Properties

| Property | Description |
|----------|-------------|
| `spring.cloud.stream.bindings.<binding-name>-out-0.destination` | Kafka topic name for the producer |
| `spring.cloud.stream.bindings.<binding-name>-in-0.destination` | Kafka topic name for the consumer |
| `spring.cloud.stream.bindings.<binding-name>-in-0.group` | Consumer group name |
| `spring.cloud.stream.kafka.binder.brokers` | Kafka broker address(es) |
| `spring.integration.poller.fixed-delay` | Interval between `Supplier` invocations (milliseconds) |

## Project Structure

```
kafka-examples/
├── docker-compose.yaml          # Kafka + Kafka UI services
├── consumer/                    # Spring Boot consumer app
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       └── resources/
│   ├── pom.xml
│   └── mvnw
├── producer/                    # Spring Boot producer app
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       └── resources/
│   ├── pom.xml
│   └── mvnw
└── README.MD
```

## Key Concepts Demonstrated

- **Functional Programming Model**: Writing the producer as a `Supplier` bean and the consumer as a `Consumer` bean.
- **Spring Cloud Stream Abstractions**: Using functional bindings to decouple application logic from messaging infrastructure.
- **Consumer Groups**: Configuring `group` to enable scalable, competing consumer behavior.
- **Poller-Based Message Emission**: Controlling producer frequency via `spring.integration.poller.fixed-delay`.
- **Kafka Binder Configuration**: Setting broker addresses and topic destinations through `application.yaml`.

## Monitoring

Kafka UI is available at http://localhost:8090. You can use it to:

- Inspect the `my-topic` topic and its partitions.
- View message throughput and consumer group lag.
- Browse individual messages as they are produced.

## License

This project is provided for educational purposes. Feel free to use it as a starting point for your own Spring Cloud Stream and Kafka experiments.

---

# مثالهای Kafka (Spring Cloud Stream)

یک پروژه مینیمال Spring Boot که ارسال پیام ناهمزمان بین دو سرویس را با استفاده از **Spring Cloud Stream** و **Apache Kafka** به عنوان کارگزار پیام نشان میدهد. این اپلیکیشن از مدل برنامهنویسی تابعی با beanهای `Supplier` و `Consumer` برای جدا کردن منطق کسبوکار از زیرساخت پیامرسانی استفاده میکند.

## نمای کلی

این مخزن شامل دو اپلیکیشن مستقل Spring Boot است:

- **Producer (تولیدکننده)** – پیامها را از طریق Spring Cloud Stream به یک تاپیک Kafka با استفاده از یک bean تابعی `Supplier` منتشر میکند.
- **Consumer (مصرفکننده)** – به همان تاپیک Kafka گوش میدهد و پیامهای دریافتی را با استفاده از یک bean `Consumer` پردازش میکند.

Spring Cloud Stream لایهی واسط پیامرسانی را انتزاع میکند، بنابراین کد اپلیکیشن روی منطق کسبوکار تمرکز دارد نه روی APIهای زیرین Kafka. وابستگی `spring-cloud-stream-binder-kafka` فریمورک را به کارگزار متصل میکند. این شاخه جریان اصلی Producer–Consumer را با استفاده از سبک برنامهنویسی تابعی Spring Cloud Stream نشان میدهد.

## پیشنیازها

- جاوا ۱۷ یا بالاتر
- Maven 3.9+
- Docker و Docker Compose (برای اجرای Kafka)

## شروع به کار

### ۱. راهاندازی Kafka

فایل `docker-compose.yaml` برای راهاندازی یک نمونه Kafka همراه با Kafka UI برای بررسی آسان تاپیکها فراهم شده است.

```bash
docker compose up -d
```

Kafka UI در آدرس http://localhost:8090 در دسترس خواهد بود و کارگزار Kafka از طریق `localhost:9092` قابل دسترسی است.

### ۲. اجرای Producer

به پوشه `producer` بروید و اپلیکیشن را اجرا کنید:

```bash
cd producer
./mvnw spring-boot:run
```

Producer از یک bean تابعی `Supplier` برای انتشار پیامها به یک اتصال خروجی Spring Cloud Stream استفاده میکند. به طور پیشفرض، poller هر ۵ ثانیه یکبار `Supplier` را فراخوانی میکند. این مقدار را میتوان از طریق خاصیت `spring.integration.poller.fixed-delay` تنظیم کرد.

### ۳. اجرای Consumer

در یک ترمینال جداگانه، به پوشه `consumer` بروید و اپلیکیشن را اجرا کنید:

```bash
cd consumer
./mvnw spring-boot:run
```

Consumer به اتصال ورودی متناظر گوش میدهد. پیامهای دریافتی از طریق یک bean `Consumer` در کنسول لاگ میشوند. Consumer پیامها را به محض دریافت پردازش میکند؛ اگر Producer سریعتر از Consumer پیام ارسال کند، پیامها در تاپیک Kafka انباشته میشوند.

## تنظیمات

هر دو اپلیکیشن از مدل برنامهنویسی تابعی Spring Cloud Stream با نامهای اتصال برگرفته از نام bean (`myMessageCreated`) استفاده میکنند. آدرس کارگزار Kafka به طور پیشفرض روی `localhost:9092` تنظیم شده است.

### تنظیمات Producer (`producer/src/main/resources/application.yaml`)

```yaml
server:
port: 8081

spring:
application:
name: producer
cloud:
function:
definition: myMessageCreated
stream:
bindings:
myMessageCreated-out-0:
destination: my-topic
content-type: application/json
kafka:
binder:
brokers: localhost:9092
integration:
poller:
fixed-delay: 5000
```

خاصیت `spring.integration.poller.fixed-delay` کنترل میکند که هر چند وقت یکبار `Supplier` فراخوانی شود. مقدار `5000` را به هر مقدار میلیثانیهای تغییر دهید تا فرکانس پیام تنظیم شود.

### تنظیمات Consumer (`consumer/src/main/resources/application.yaml`)

```yaml
server:
port: 8082

spring:
application:
name: consumer
cloud:
function:
definition: myMessageCreated
stream:
bindings:
myMessageCreated-in-0:
destination: my-topic
group: notification-group
kafka:
binder:
brokers: localhost:9092
```

خاصیت `group` تضمین میکند که Consumer به یک گروه مصرفکننده خاص (`notification-group`) تعلق دارد و امکان مصرف مقیاسپذیر پیامها را فراهم میکند.

### خواص کلیدی تنظیمات

| خاصیت | توضیح |
|-------|-------|
| `spring.cloud.stream.bindings.<binding-name>-out-0.destination` | نام تاپیک Kafka برای Producer |
| `spring.cloud.stream.bindings.<binding-name>-in-0.destination` | نام تاپیک Kafka برای Consumer |
| `spring.cloud.stream.bindings.<binding-name>-in-0.group` | نام گروه مصرفکننده |
| `spring.cloud.stream.kafka.binder.brokers` | آدرس کارگزار(های) Kafka |
| `spring.integration.poller.fixed-delay` | بازه بین فراخوانیهای `Supplier` (میلیثانیه) |

## ساختار پروژه

```
kafka-examples/
├── docker-compose.yaml          # سرویسهای Kafka + Kafka UI
├── consumer/                    # اپلیکیشن مصرفکننده Spring Boot
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       └── resources/
│   ├── pom.xml
│   └── mvnw
├── producer/                    # اپلیکیشن تولیدکننده Spring Boot
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       └── resources/
│   ├── pom.xml
│   └── mvnw
└── README.MD
```

## مفاهیم کلیدی نشان داده شده

- **مدل برنامهنویسی تابعی**: نوشتن Producer به صورت bean `Supplier` و Consumer به صورت bean `Consumer`.
- **انتزاعهای Spring Cloud Stream**: استفاده از اتصالات تابعی برای جدا کردن منطق اپلیکیشن از زیرساخت پیامرسانی.
- **گروههای مصرفکننده**: پیکربندی `group` برای فعال کردن رفتار مصرفکننده رقابتی و مقیاسپذیر.
- **انتشار پیام مبتنی بر Poller**: کنترل فرکانس Producer از طریق `spring.integration.poller.fixed-delay`.
- **پیکربندی Kafka Binder**: تنظیم آدرس کارگزار و مقاصد تاپیک از طریق `application.yaml`.

## پایش

Kafka UI در آدرس http://localhost:8090 در دسترس است. میتوانید از آن برای موارد زیر استفاده کنید:

- بررسی تاپیک `my-topic` و پارتیشنهای آن.
- مشاهده توان عملیاتی پیامها و تأخیر گروه مصرفکننده.
- مرور پیامهای تکتک هنگام تولید.

## مجوز

این پروژه برای اهداف آموزشی ارائه شده است. استفاده از آن به عنوان نقطه شروع برای آزمایشهای Spring Cloud Stream و Kafka خودتان آزاد است.