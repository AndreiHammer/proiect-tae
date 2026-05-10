package com.hammer.proiecttae.service;

import com.hammer.proiecttae.util.KafkaTopicsConstants;
import com.hammer.proiecttae.util.MessageUtils;
import com.hammer.proiecttae.util.XsltTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.Map;

import static com.hammer.proiecttae.util.MessageUtils.extractHeader;

@Slf4j
@Service
@RequiredArgsConstructor
@Profile("payment-to-notification")
public class PaymentConsumerService {

    private static final String PAYMENT_TO_NOTIFICATION_XSL = "/xsl/payment-to-notification.xsl";

    private final XsltTransformer xsltTransformer;
    private final NotificationProducerService notificationProducerService;

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

        try {
            log.info("Applying XSLT transformation '{}'...", PAYMENT_TO_NOTIFICATION_XSL);
            String notificationXml = xsltTransformer.transform(
                    record.value(), PAYMENT_TO_NOTIFICATION_XSL, Map.of()
            );

            String notificationKey = extractNotificationId(notificationXml, record.key());
            notificationProducerService.sendNotification(notificationKey, notificationXml, correlationId);

            log.info("[{}]=== [PAYMENTS] Processing complete — key='{}'; notificationKey='{}', correlationId='{}' ===",
                    correlationId, record.key(), notificationKey, correlationId);

        } catch (Exception e) {
            log.error("=== [PAYMENTS] ERROR processing message key='{}', correlationId='{}' ===",
                    record.key(), correlationId, e);
        }
    }

    private String extractNotificationId(String notificationXml, String fallbackKey) {
        try {
            var document = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(new org.xml.sax.InputSource(new StringReader(notificationXml)));
            String notificationId = (String) XPathFactory.newInstance().newXPath()
                    .evaluate("/Notification/notificationId", document, XPathConstants.STRING);
            if (notificationId != null && !notificationId.isBlank()) return notificationId;
        } catch (Exception e) {
            log.warn("Could not extract notificationId from transformed XML, using fallback key '{}'", fallbackKey, e);
        }
        return "NOTIF-" + fallbackKey;
    }
}
