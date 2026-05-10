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
@Profile("payment-to-notification")
public class NotificationProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendNotification(String notificationKey, String notificationXml, String correlationId) {
        log.info("Forwarding notification [{}] to topic '{}' (correlationId={})",
                notificationKey, KafkaTopicsConstants.NOTIFICATIONS, correlationId);
        log.debug("Notification XML to be sent:\n{}", notificationXml);

        Message<String> message = MessageBuilder
                .withPayload(notificationXml)
                .setHeader(KafkaHeaders.TOPIC, KafkaTopicsConstants.NOTIFICATIONS)
                .setHeader(KafkaHeaders.KEY, notificationKey)
                .setHeader(MessageUtils.CORRELATION_ID, correlationId)
                .setHeader(MessageUtils.MESSAGE_TYPE, "Notification")
                .setHeader(MessageUtils.SENT_AT, Instant.now().toString())
                .build();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Notification [{}] sent successfully → topic='{}', partition={}, offset={}, correlationId={}",
                        notificationKey,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        correlationId);
            } else {
                log.error("Failed to forward notification [{}] to topic '{}' (correlationId={})",
                        notificationKey, KafkaTopicsConstants.NOTIFICATIONS, correlationId, ex);
            }
        });
    }

}
