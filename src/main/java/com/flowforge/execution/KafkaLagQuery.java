package com.flowforge.execution;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Read-only Kafka offset sampling. Uncommitted dispatch partitions use their
 * earliest retained offset, matching the worker's auto-offset-reset=earliest.
 * DLT retention is NOT a count of unprocessed or unique poison messages.
 */
final class KafkaLagQuery {
    private static final long TIMEOUT_SECONDS = 5;
    private final AdminClient admin;
    private final String dispatchTopic;
    private final String dltTopic;
    private final String groupId;

    record Snapshot(long dispatchLag, long dltRetained) {}

    KafkaLagQuery(AdminClient admin, String dispatchTopic, String dltTopic, String groupId) {
        if (admin == null || dispatchTopic == null || dispatchTopic.isBlank()
                || dltTopic == null || dltTopic.isBlank() || dispatchTopic.equals(dltTopic)
                || groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Kafka lag sampler requires distinct topics and a consumer group");
        }
        this.admin = admin;
        this.dispatchTopic = dispatchTopic;
        this.dltTopic = dltTopic;
        this.groupId = groupId;
    }

    Snapshot read() throws InterruptedException, ExecutionException, TimeoutException {
        var descriptions = admin.describeTopics(List.of(dispatchTopic, dltTopic))
                .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        var partitions = new ArrayList<TopicPartition>();
        for (String topic : List.of(dispatchTopic, dltTopic)) {
            var description = descriptions.get(topic);
            if (description == null || description.partitions().isEmpty()) {
                throw new IllegalStateException("Missing or partitionless Kafka topic: " + topic);
            }
            description.partitions().forEach(p ->
                    partitions.add(new TopicPartition(topic, p.partition())));
        }

        Map<TopicPartition, OffsetSpec> earliestRequests = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latestRequests = new HashMap<>();
        for (TopicPartition partition : partitions) {
            earliestRequests.put(partition, OffsetSpec.earliest());
            latestRequests.put(partition, OffsetSpec.latest());
        }
        var earliestResults = admin.listOffsets(earliestRequests).all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        var latestResults = admin.listOffsets(latestRequests).all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        var groupOffsets = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Map<TopicPartition, Long> earliest = new HashMap<>();
        Map<TopicPartition, Long> latest = new HashMap<>();
        for (TopicPartition partition : partitions) {
            var first = earliestResults.get(partition);
            var last = latestResults.get(partition);
            if (first == null || last == null) {
                throw new IllegalStateException("Kafka offset missing for " + partition);
            }
            earliest.put(partition, first.offset());
            latest.put(partition, last.offset());
        }
        Map<TopicPartition, Long> committed = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
            if (entry.getValue() != null) {
                committed.put(entry.getKey(), entry.getValue().offset());
            }
        }
        return calculate(dispatchTopic, dltTopic, earliest, latest, committed);
    }

    static Snapshot calculate(String dispatchTopic, String dltTopic,
                              Map<TopicPartition, Long> earliest,
                              Map<TopicPartition, Long> latest,
                              Map<TopicPartition, Long> committed) {
        long lag = 0;
        long retained = 0;
        int dispatchPartitions = 0;
        int dltPartitions = 0;
        for (var entry : earliest.entrySet()) {
            TopicPartition partition = entry.getKey();
            if (!partition.topic().equals(dispatchTopic) && !partition.topic().equals(dltTopic)) {
                continue;
            }
            Long end = latest.get(partition);
            long start = entry.getValue();
            if (end == null || start < 0 || end < start) {
                throw new IllegalStateException("Invalid Kafka offsets for " + partition);
            }
            if (partition.topic().equals(dispatchTopic)) {
                dispatchPartitions++;
                long next = Math.max(start, committed.getOrDefault(partition, start));
                lag = Math.addExact(lag, Math.max(0L, end - next));
            } else {
                dltPartitions++;
                retained = Math.addExact(retained, end - start);
            }
        }
        if (dispatchPartitions == 0 || dltPartitions == 0) {
            throw new IllegalStateException("Kafka topic partitions are missing");
        }
        return new Snapshot(lag, retained);
    }
}
