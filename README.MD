# ریشه‌یابی مشکل: تولیدکننده به Topic اشتباه پیام می‌فرستد

## تشخیص دقیق مشکل

با بررسی دقیق فایل‌های شما، یک **عدم تطابق حیاتی بین Binding تعریف‌شده و نامی که StreamBridge با آن فراخوانی می‌شود** وجود دارد. این دقیقاً دلیل اصلی دریافت نشدن پیام توسط Consumer است.

فایل `ProducerFunctions.java` شما:

```java
public void sendMyMessage() {
String message = "message" + UUID.randomUUID();
streamBridge.send("myMessageCreated", message);   // ← مشکل اینجاست
System.out.println("Sending message : " + message);
}
```

در حالی که فایل `application.yaml` شما این Binding را تعریف کرده است:

```yaml
spring:
cloud:
stream:
bindings:
myMessageCreated-out-0:        # ← نام Binding
destination: my-topic         # ← مقصد (Topic)
```

## چرا این مشکل رخ می‌دهد؟

رفتار `StreamBridge.send()` را باید به دقت شناخت:

- اگر نامی که به `send()` پاس می‌دهید **دقیقاً با نام یک Binding تعریف‌شده در `application.yaml`** مطابقت داشته باشد، Spring Cloud Stream از تنظیمات آن Binding (از جمله `destination`) استفاده می‌کند.
- اما اگر نامی که پاس می‌دهید **هیچ Binding تعریف‌شده‌ای نداشته باشد**، Spring Cloud Stream **یک Binding جدید به‌صورت داینامیک می‌سازد** و نام پاس‌داده‌شده را به‌عنوان **نام مستقیم مقصد (Destination)** در نظر می‌گیرد.

در کد شما، نام پاس‌داده‌شده `"myMessageCreated"` است، در حالی که Binding تعریف‌شده `"myMessageCreated-out-0"` است. این دو با هم مطابقت ندارند. در نتیجه:

- Spring Cloud Stream یک Binding داینامیک با Destination = `myMessageCreated` می‌سازد.
- پیام شما به Kafka Topic‌ای با نام **`myMessageCreated`** ارسال می‌شود، **نه** به `my-topic`.
- Consumer شما که به `my-topic` گوش می‌دهد، هیچ پیامی دریافت نمی‌کند.

به بیان دیگر، **Producer شما در واقع کار می‌کند و پیام را می‌فرستد، اما به یک Topic اشتباه**.

## راه‌حل: استفاده از نام کامل Binding

فایل `ProducerFunctions.java` را به شکل زیر اصلاح کنید:

```java
package com.example.producer.functions;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProducerFunctions {

    private final StreamBridge streamBridge;

    public void sendMyMessage() {
        String message = "message-" + UUID.randomUUID();
        streamBridge.send("myMessageCreated-out-0", message);   // ← نام کامل Binding
        System.out.println("Sending message : " + message);
    }
}
```

با این تغییر، Spring Cloud Stream از تنظیمات `myMessageCreated-out-0` استفاده می‌کند و پیام را به `my-topic` (همان Topic که Consumer به آن گوش می‌دهد) ارسال می‌کند.

## تأیید مشکل با بررسی Topicهای Kafka

برای اطمینان از صحت این تشخیص، می‌توانید لیست Topicهای موجود در Kafka را بررسی کنید:

```bash
docker exec -it kafka kafka-topics \
--list \
--bootstrap-server localhost:9092
```

خروجی احتمالاً شامل دو Topic زیر خواهد بود:

```text
my-topic
myMessageCreated
```

Topic `my-topic` توسط Consumer گوش داده می‌شود و خالی است، در حالی که Topic `myMessageCreated` توسط Producer به‌طور اشتباه ایجاد شده و پیام‌ها را در خود جای داده است. با دستور زیر می‌توانید محتوای Topic اشتباه را ببینید:

```bash
docker exec -it kafka kafka-console-consumer \
--topic myMessageCreated \
--bootstrap-server localhost:9092 \
--from-beginning \
--max-messages 5
```

## نکته‌ی مهم: نام Binding در مقابل نام تابع

برای جلوگیری از این‌گونه اشتباهات در آینده، این قاعده‌ی ساده را به خاطر بسپارید:

| موجودیت | نام‌گذاری | مثال |
|---------|----------|------|
| Bean تابع | دلخواه (معمولاً با نام مفهومی) | `myMessageCreated` |
| Binding در YAML | باید از الگوی `<functionName>-in-<index>` یا `<functionName>-out-<index>` پیروی کند | `myMessageCreated-out-0` |
| فراخوانی StreamBridge | **نام کامل Binding**، نه نام تابع | `streamBridge.send("myMessageCreated-out-0", payload)` |

بنابراین هر زمان که از `StreamBridge` استفاده می‌کنید، **همیشه نام Binding کامل (با پسوند `-out-<index>`)** را پاس دهید، نه نام تابع.

## بهبودهای جانبی

### ۱. ارسال چندین پیام برای تست

در حال حاضر `CommandLineRunner` تنها **یک بار** و در زمان راه‌اندازی برنامه یک پیام ارسال می‌کند. برای تست بهتر، می‌توانید آن را به‌شکل زیر تغییر دهید تا هر چند ثانیه یک پیام بفرستد:

```java
@Bean
protected CommandLineRunner commandLineRunner(ProducerFunctions producerFunctions) {
return args -> {
for (int i = 0; i < 5; i++) {
producerFunctions.sendMyMessage();
Thread.sleep(3000);
}
};
}
```

این تغییر به شما اجازه می‌دهد تا در طول چند ثانیه، چندین پیام را ارسال کرده و رفتار Consumer را مشاهده کنید.

### ۲. اصلاح Content-Type در Producer

در `application.yaml` Producer، `content-type: application/json` تعیین شده است، در حالی که پیام شما یک **String ساده** است. این تنظیم باعث می‌شود رشته‌ی شما به‌صورت JSON Serialize شود (یعنی با علامت نقل‌قول: `"message-abc123"`)، که ممکن است در Consumer به‌صورت غیرمنتظره ظاهر شود. برای ارسال رشته‌های ساده، بهتر است از `text/plain` استفاده کنید:

```yaml
spring:
cloud:
stream:
bindings:
myMessageCreated-out-0:
destination: my-topic
content-type: text/plain
```

یا اگر قصد ارسال اشیاء Java را دارید (مثلاً کلاس `OrderCreatedEvent`)، `application/json` مناسب است، اما در آن صورت نوع ورودی Consumer نیز باید همان کلاس باشد.

### ۳. فعال‌سازی لاگ‌های Debug

برای مشاهده‌ی دقیق فرآیند Binding و اطمینان از اتصال صحیح، در هر دو سرویس Producer و Consumer این تنظیمات را اضافه کنید:

```yaml
logging:
level:
org.springframework.cloud.stream: DEBUG
org.springframework.kafka: INFO
org.apache.kafka: WARN
```

در لاگ‌های Producer باید خطوطی مشابه زیر مشاهده کنید که نشان می‌دهد Binding به‌درستی ساخته شده است:

```text
o.s.c.s.binder.DefaultBinderFactory : Creating binder: kafka
o.s.c.s.b.k.KafkaMessageChannelBinder : New binding created: myMessageCreated-out-0 -> my-topic
o.s.k.l.KafkaProducerFactory          : Producer created for topic my-topic
```

در لاگ‌های Consumer نیز باید خط زیر را ببینید:

```text
o.s.k.l.KafkaMessageListenerContainer : partitions assigned: [my-topic-0]
```

وجود این پیام تأیید می‌کند که Consumer با موفقیت به Topic متصل شده است.

## بررسی نهایی: چک‌لیست عیب‌یابی

اگر پس از این تغییرات همچنان Consumer پیامی دریافت نکرد، موارد زیر را به ترتیب بررسی کنید:

- **آیا Topic `my-topic` در Kafka وجود دارد؟** اگر تنظیم `KAFKA_AUTO_CREATE_TOPICS_ENABLE` روی `false` باشد، باید Topic را دستی ایجاد کنید.
- **آیا Producer و Consumer به یک Kafka Broker متصل می‌شوند؟** هر دو باید `localhost:9092` را در تنظیمات داشته باشند.
- **آیا Consumer Group یکسان است؟** اگر Consumer Group متفاوت باشد، پیام‌ها بین آن‌ها تقسیم می‌شوند و ممکن است یک Consumer پیام دریافت نکند.
- **آیا Offset قدیمی مانع از خواندن پیام‌های جدید می‌شود؟** اگر Consumer قبلاً روی همان Topic و Group اجرا شده باشد و Offset آن ذخیره شده باشد، ممکن است پیام‌های جدید را نبیند. با تغییر `group` به یک نام جدید، می‌توانید این موضوع را تست کنید.

## جمع‌بندی

مشکل اصلی در **عدم تطابق نام Binding** بود:

- Producer در `application.yaml` Binding را با نام `myMessageCreated-out-0` تعریف کرده بود.
- اما در کد، StreamBridge را با نام `myMessageCreated` فراخوانی می‌کرد.
- در نتیجه، پیام‌ها به Topic اشتباه (`myMessageCreated`) می‌رفتند، نه به `my-topic` که Consumer منتظر آن بود.

راه‌حل: در فراخوانی `StreamBridge.send()`، **همیشه نام کامل Binding** (مطابق با الگوی `<functionName>-out-<index>`) را پاس دهید. با این تغییر، Producer و Consumer شما به یکدیگر متصل خواهند شد و پیام‌ها به‌درستی منتقل می‌شوند.