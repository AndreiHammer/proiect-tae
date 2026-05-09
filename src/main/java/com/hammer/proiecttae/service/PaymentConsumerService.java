package com.hammer.proiecttae.service;

import com.hammer.proiecttae.util.KafkaTopicsConstants;
import com.hammer.proiecttae.util.MessageUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static com.hammer.proiecttae.util.MessageUtils.extractHeader;

@Slf4j
@Service
@Profile("payment-consumer")
public class PaymentConsumerService {

    @KafkaListener(
            topics = KafkaTopicsConstants.PAYMENTS,
            groupId = "xslt-payment-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePayment(ConsumerRecord<String, String> record) {
        String correlationId = extractHeader(record, MessageUtils.CORRELATION_ID).orElse("unknown");
        String messageType   = extractHeader(record, MessageUtils.MESSAGE_TYPE).orElse("-");
        String sentAt        = extractHeader(record, MessageUtils.SENT_AT).orElse("-");

        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║          [PAYMENTS] ✔ Transformed Payment Received           ║");
        log.info("╠══════════════════════════════════════════════════════════════╣");
        log.info("  Topic         : {}", record.topic());
        log.info("  Partition     : {}", record.partition());
        log.info("  Offset        : {}", record.offset());
        log.info("  Key           : {}", record.key());
        log.info("  correlationId : {}", correlationId);
        log.info("  messageType   : {}", messageType);
        log.info("  sentAt        : {}", sentAt);
        log.info("  Payload       :");

        for (String line : record.value().split("\n")) {
            if (!line.isBlank()) log.info("    {}", line);
        }
        log.info("╚══════════════════════════════════════════════════════════════╝");
    }
}
