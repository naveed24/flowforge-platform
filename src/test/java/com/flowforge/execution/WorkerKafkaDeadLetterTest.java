package com.flowforge.execution;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WorkerKafkaDeadLetterTest {

    @Test
    void dispatchSerializerKeepsMalformedBytesExactAndSerializesValidEventsAsJson() {
        try (var serializer = new DispatchOrRawValueSerializer()) {
            byte[] malformed = new byte[] {0, (byte) 0xff, 0x12, 0x0a};
            assertThat(serializer.serialize("dispatch.DLT", malformed)).isEqualTo(malformed);
            assertThat(serializer.serialize("dispatch.DLT", new RecordHeaders(), malformed))
                    .isEqualTo(malformed);
            assertThat(serializer.serialize("dispatch.DLT", (Object) null)).isNull();

            var event = new ExecutionDispatchMessage(19, 2, 91);
            String json = new String(serializer.serialize("dispatch", event), StandardCharsets.UTF_8);
            assertThat(json).contains("\"executionId\":19", "\"attemptNumber\":2", "\"outboxId\":91");
        }
    }

    @Test
    void malformedJsonIsCapturedByErrorHandlingDeserializer() {
        var headers = new RecordHeaders();
        byte[] invalid = "{not-json".getBytes(StandardCharsets.UTF_8);
        try (var deserializer = new ErrorHandlingDeserializer<>(
                new JacksonJsonDeserializer<>(ExecutionDispatchMessage.class))) {
            assertThat(deserializer.deserialize("dispatch", headers, invalid)).isNull();
            assertThat(headers.toArray()).isNotEmpty();
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void invalidRecordIsDeadLetteredBeforeOffsetCanBeRecovered() {
        KafkaTemplate<String, ExecutionDispatchMessage> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        var handler = new WorkerKafkaErrorConfiguration()
                .workerDispatchErrorHandler(kafka, "flowforge.execution.dispatch.v1.DLT");
        var record = new ConsumerRecord<>("flowforge.execution.dispatch.v1", 2, 12L,
                "bad-key", new ExecutionDispatchMessage(10, 1, 100));

        assertThat(handler.isAckAfterHandle()).isTrue();
        assertThat(handler.handleOne(new IllegalArgumentException("invalid key"), record,
                mock(Consumer.class), mock(MessageListenerContainer.class))).isTrue();

        var capture = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(capture.capture());
        assertThat(capture.getValue().topic()).isEqualTo("flowforge.execution.dispatch.v1.DLT");
        assertThat(capture.getValue().key()).isEqualTo("bad-key");
        assertThat(capture.getValue().value()).isEqualTo(record.value());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void dltSendFailureDoesNotCountAsRecovered() {
        KafkaTemplate<String, ExecutionDispatchMessage> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var handler = new WorkerKafkaErrorConfiguration()
                .workerDispatchErrorHandler(kafka, "flowforge.execution.dispatch.v1.DLT");
        var record = new ConsumerRecord<>("flowforge.execution.dispatch.v1", 0, 13L,
                "bad-key", new ExecutionDispatchMessage(10, 1, 100));

        assertThatThrownBy(() -> handler.handleOne(new IllegalArgumentException("invalid key"),
                record, mock(Consumer.class), mock(MessageListenerContainer.class)))
                .isInstanceOf(RuntimeException.class);
        verify(kafka).send(any(ProducerRecord.class));
    }
}
