# proiect-tae

An order-to-cash pipeline built with Spring Boot and Apache Kafka. XML messages flow through three transformation stages, each driven by an XSLT stylesheet. The application runs as four independent containers, each loaded with a different Spring profile, mimicking a microservice architecture without the overhead of separate codebases.

---

## Workflow

A single REST call triggers the full pipeline. Each stage consumes a message from one Kafka topic, applies an XSLT transformation, and produces the result to the next topic.

```
GET /api/workflow/start
        |
        v
[invoice-ingest]
  Builds a sample Invoice, marshals it to XML,
  attaches tracing headers (correlationId, messageType, sentAt),
  and produces it to the 'invoices' topic.
        |
        v  invoices topic
        |
[invoice-to-payment]
  Consumes the Invoice XML.
  Applies invoice-to-payment.xsl:
    - paymentId     = "PAY-" + invoiceId
    - payee         = vendor
    - payer         = customer
    - netAmount     = amount
    - paymentStatus = "APPROVED"
  Produces the Payment XML to the 'payments' topic.
        |
        v  payments topic
        |
[payment-to-notification]
  Consumes the Payment XML.
  Applies payment-to-notification.xsl:
    - notificationId = "NOTIF-" + paymentId
    - recipient      = payer
    - subject        = "Payment confirmation for " + reference
    - status         = paymentStatus
  Produces the Notification XML to the 'notifications' topic.
        |
        v  notifications topic
        |
[notification-dispatch]
  Consumes the Notification XML.
  Unmarshals it into a typed Java object via JAXB.
  Logs each field of the Notification — end of pipeline.
```

The `correlationId` header is generated at the first step and propagated unchanged across all three topics, so every log line in every container can be tied back to the same workflow invocation.

---

## Tech stack

- Java 21
- Spring Boot 3; Gradle
- Spring Kafka
- Apache Kafka (KRaft mode)
- JAXB (marshal / unmarshal XML)
- XSLT 1.0 (javax.xml.transform)
- Lombok
- Docker Compose
- AKHQ (Kafka UI)

---

## Spring profiles

Each container runs the same JAR with a different `SPRING_PROFILES_ACTIVE` value. Only the beans annotated with the matching `@Profile` are loaded.

| Profile | Active beans | Kafka role |
|---|---|---|
| `invoice-ingest` | InvoiceProducerService, WorkflowService, WorkflowApi | producer → invoices |
| `invoice-to-payment` | InvoiceConsumerService, PaymentProducerService | consumer + producer |
| `payment-to-notification` | PaymentConsumerService, NotificationProducerService | consumer + producer |
| `notification-dispatch` | NotificationConsumerService | consumer (terminal) |

Infrastructure beans (`KafkaConsumerConfig`, `KafkaProducerConfig`, `XsltTransformer`) carry no profile and are loaded in every container.

---

## Kafka cluster

Two brokers running in KRaft mode. Both nodes act as broker and controller. Topics are created with `partitions=2`, `replicas=2`, and `min.insync.replicas=2`, so each partition is stored on both brokers and a write is only acknowledged once both have confirmed it.

The INTERNAL listener (`kafka1:29092`, `kafka2:29092`) is used for all inter-container communication. The PLAINTEXT listener (`localhost:9092`, `localhost:9094`) is exposed to the host for local tooling.

---

## Running locally

### Prerequisites

- Docker and Docker Compose
- The image published to Docker Hub: `andreihammer/proiect-tae:latest`

To build and push the image with Jib:

```bash
gradle jibDockerBuild
```

### Start the stack

```bash
docker compose up -d
```

The app containers depend on the Kafka brokers passing their healthcheck before starting. The healthcheck uses the full script path available in the `apache/kafka` image:

```
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:<port>
```

Allow 30-60 seconds for the brokers to complete KRaft leader election and for all containers to reach a running state.

### Trigger the workflow

```bash
curl http://localhost:8081/api/workflow/start
```

### Watch the pipeline

```bash
docker compose logs -f invoice-ingest          # invoice produced
docker compose logs -f invoice-to-payment      # XSLT: Invoice -> Payment
docker compose logs -f payment-to-notification # XSLT: Payment -> Notification
docker compose logs -f notification-dispatch   # final object logged
```

The same `correlationId` appears in all four log streams.

### Kafka UI

AKHQ is available at `http://localhost:8090`. It connects to both brokers under a single cluster view and shows topic contents, consumer group offsets, and partition assignments.

### Ports

| Service | Host port |
|---|---|
| kafka1 (PLAINTEXT) | 9092 |
| kafka2 (PLAINTEXT) | 9094 |
| AKHQ | 8090 |
| invoice-ingest | 8081 |
| invoice-to-payment | 8082 |
| payment-to-notification | 8083 |
| notification-dispatch | 8084 |

### Stop

```bash
docker compose down
```
