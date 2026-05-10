package com.hammer.proiecttae.service;

import com.hammer.proiecttae.model.Notification;
import com.hammer.proiecttae.util.KafkaTopicsConstants;
import com.hammer.proiecttae.util.MessageUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static com.hammer.proiecttae.util.MessageUtils.extractHeader;
import static com.hammer.proiecttae.util.XmlUtils.unmarshal;

@Slf4j
@Service
@Profile("notification-dispatch")
public class NotificationConsumerService {

    @KafkaListener(
            topics = KafkaTopicsConstants.NOTIFICATIONS,
            groupId = "xslt-notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeNotification(ConsumerRecord<String, String> record) {
        String correlationId = extractHeader(record, MessageUtils.CORRELATION_ID).orElse("unknown");
        String messageType   = extractHeader(record, MessageUtils.MESSAGE_TYPE).orElse("-");
        String sentAt        = extractHeader(record, MessageUtils.SENT_AT).orElse("-");

        log.info("╔══════════════════════════════════════════════════════════════════╗");
        log.info("║       [NOTIFICATIONS] ✔ Final Notification Received              ║");
        log.info("╠══════════════════════════════════════════════════════════════════╣");
        log.info("  Topic         : {}", record.topic());
        log.info("  Partition     : {}", record.partition());
        log.info("  Offset        : {}", record.offset());
        log.info("  Key           : {}", record.key());
        log.info("  correlationId : {}", correlationId);
        log.info("  messageType   : {}", messageType);
        log.info("  sentAt        : {}", sentAt);

        try {
            Notification notification = unmarshal(record.value(), Notification.class);

            log.info("╠══════════════════════════════════════════════════════════════════╣");
            log.info("[{}]  [Unmarshalled Notification Object]", correlationId);
            log.info("  notificationId  : {}", notification.getNotificationId());
            log.info("  notifiedAt      : {}", notification.getNotifiedAt());
            log.info("  recipient       : {}", notification.getRecipient());
            log.info("  subject         : {}", notification.getSubject());
            log.info("  amount          : {} {}", notification.getAmount(), notification.getCurrency());
            log.info("  status          : {}", notification.getStatus());
            log.info("  sourcePaymentId : {}", notification.getSourcePaymentId());
        } catch (Exception e) {
            log.error("[{}]Failed to unmarshal Notification XML: ", correlationId, e);
        }

        log.info("╚══════════════════════════════════════════════════════════════════╝");
    }
}
