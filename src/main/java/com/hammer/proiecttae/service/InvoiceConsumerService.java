package com.hammer.proiecttae.service;

import com.hammer.proiecttae.util.KafkaTopicsConstants;
import com.hammer.proiecttae.util.MessageUtils;
import com.hammer.proiecttae.util.XsltTranformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.Map;

import static com.hammer.proiecttae.util.MessageUtils.extractHeader;

@Slf4j
@RequiredArgsConstructor
@Service
@Profile("invoice-consumer")
public class InvoiceConsumerService {

    private static final String INVOICE_TO_PAYMENT_XSL = "/xsl/invoice-to-payment.xsl";

    private final XsltTranformer xsltTranformer;
    private final PaymentProducerService paymentProducerService;

    @KafkaListener(
            topics = KafkaTopicsConstants.INVOICES,
            groupId = "xslt-invoice-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeInvoice(final ConsumerRecord<String, String> record) {
        String correlationId = extractHeader(record, MessageUtils.CORRELATION_ID).orElse("unknown");
        String messageType   = extractHeader(record, MessageUtils.MESSAGE_TYPE).orElse("-");
        String sentAt        = extractHeader(record, MessageUtils.SENT_AT).orElse("-");

        log.info("=== [INVOICES] Message received ===");
        log.info("  Topic         : {}", record.topic());
        log.info("  Partition     : {}", record.partition());
        log.info("  Offset        : {}", record.offset());
        log.info("  Key           : {}", record.key());
        log.info("  correlationId : {}", correlationId);
        log.info("  messageType   : {}", messageType);
        log.info("  sentAt        : {}", sentAt);
        log.debug("  Payload       :\n{}", record.value());

        try {
            log.info("Applying XSLT transformation '{}'...", INVOICE_TO_PAYMENT_XSL);
            String paymentXml = xsltTranformer.transform(record.value(), INVOICE_TO_PAYMENT_XSL, Map.of());

            String paymentKey = extractPaymentId(paymentXml, record.key());

            paymentProducerService.sendPayment(paymentKey, paymentXml, correlationId);

            log.info("=== [INVOICES] Processing complete for key='{}'; paymentKey='{}' ===",
                    record.key(), paymentKey);

        } catch (Exception e) {
            log.error("=== [INVOICES] ERROR processing message key='{}' from offset={} ===",
                    record.key(), record.offset(), e);
        }
    }

    private String extractPaymentId(String paymentXml, String fallbackKey) {
        try {
            var factory = DocumentBuilderFactory.newInstance();
            var builder = factory.newDocumentBuilder();
            var document = builder.parse(new InputSource(new StringReader(paymentXml)));

            XPath xPath = XPathFactory.newInstance().newXPath();
            String paymentId = (String) xPath.evaluate("/Payment/paymentId", document, XPathConstants.STRING);

            if (paymentId != null && !paymentId.isBlank()) {
                return paymentId;
            }
        } catch (Exception e) {
            log.warn("Could not extract paymentId from transformed XML, using fallback key '{}'", fallbackKey, e);
        }
        return "PAY-" + fallbackKey;
    }
}
