package com.flowforge.execution;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;

/**
 * Normal dispatches are JSON; a deserialization failure is sent to the DLT
 * as its original raw bytes. Never JSON-encode a poisoned record's byte[].
 */
public final class DispatchOrRawValueSerializer extends DelegatingByTypeSerializer {

    public DispatchOrRawValueSerializer() {
        super(Map.of(
                ExecutionDispatchMessage.class, new JacksonJsonSerializer<ExecutionDispatchMessage>(),
                byte[].class, new ByteArraySerializer()));
    }

    @Override
    public byte[] serialize(String topic, Object value) {
        return value == null ? null : super.serialize(topic, value);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object value) {
        return value == null ? null : super.serialize(topic, headers, value);
    }
}
