package com.hammer.proiecttae.service;

import com.hammer.proiecttae.util.KafkaTopicsConstants;
import com.hammer.proiecttae.util.MessageUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
@Service
@Profile("invoice-consumer")
// Activated alongside invoice-consumer because the consumer directly calls this
// to forward the transformed payload
public class PaymentProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendPayment(String paymentKey, String paymentXml, String correlationId) {
        log.info("Forwarding transformed payment [{}] to topic '{}'",
                paymentKey, KafkaTopicsConstants.PAYMENTS);
        log.debug("Payment XML to be sent:\n{}", paymentXml);

        Message<String> message = MessageBuilder
                .withPayload(paymentXml)
                .setHeader(KafkaHeaders.TOPIC, KafkaTopicsConstants.PAYMENTS)
                .setHeader(KafkaHeaders.KEY, paymentKey)
                .setHeader(MessageUtils.CORRELATION_ID, correlationId)
                .setHeader(MessageUtils.MESSAGE_TYPE, "Payment")
                .setHeader(MessageUtils.SENT_AT, Instant.now().toString())
                .build();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Payment [{}] sent successfully → topic='{}', partition={}, offset={}",
                        paymentKey,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to forward payment [{}] to topic '{}'",
                        paymentKey, KafkaTopicsConstants.PAYMENTS, ex);
            }
        });
    }
}
