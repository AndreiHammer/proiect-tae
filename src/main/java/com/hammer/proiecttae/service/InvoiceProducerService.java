package com.hammer.proiecttae.service;

import com.hammer.proiecttae.util.KafkaTopicsConstants;
import com.hammer.proiecttae.model.Invoice;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hammer.proiecttae.util.XmlUtils.marshal;

@Slf4j
@RequiredArgsConstructor
@Service
@Profile("invoice-ingest")
public class InvoiceProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendInvoice(Invoice invoice) {
        log.info("Preparing to send invoice [{}] to topic '{}'",
                invoice.getInvoiceId(), KafkaTopicsConstants.INVOICES);

        String correlationId = UUID.randomUUID().toString();

        String xmlPayload = marshal(invoice, Invoice.class);

        Message<String> message = MessageBuilder
                .withPayload(xmlPayload)
                .setHeader(KafkaHeaders.TOPIC, KafkaTopicsConstants.INVOICES)
                .setHeader(KafkaHeaders.KEY, invoice.getInvoiceId())
                .setHeader(MessageUtils.CORRELATION_ID, correlationId)
                .setHeader(MessageUtils.MESSAGE_TYPE, "Invoice")
                .setHeader(MessageUtils.SENT_AT, Instant.now().toString())
                .build();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(message);

        future.whenComplete((result, ex) -> {
           if (ex == null) {
               log.info("[{}]Invoice [{}] sent successfully: topic='{}', partition={}, offset={}",
                       correlationId,
                       invoice.getInvoiceId(),
                       result.getRecordMetadata().topic(),
                       result.getRecordMetadata().partition(),
                       result.getRecordMetadata().offset());
           } else {
               log.error("Failed to send invoice [{}] to topic '{}'",
                       invoice.getInvoiceId(), KafkaTopicsConstants.INVOICES, ex);
           }
        });
    }

}
