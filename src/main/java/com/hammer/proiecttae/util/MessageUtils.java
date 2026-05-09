package com.hammer.proiecttae.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class MessageUtils {

    private MessageUtils() {}

    public static final String CORRELATION_ID = "correlationId";

    public static final String MESSAGE_TYPE = "messageType";

    public static final String SENT_AT = "sentAt";

    public static Optional<String> extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) return Optional.empty();
        return Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }
}
