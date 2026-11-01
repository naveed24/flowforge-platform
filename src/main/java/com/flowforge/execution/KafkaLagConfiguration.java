package com.flowforge.execution;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

/**
 * Optional read-only broker telemetry. Reuses KafkaAdmin's bootstrap,
 * authentication and TLS properties rather than creating an insecure client.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "flowforge.worker.consumer.lag",
        name = "enabled", havingValue = "true")
public class KafkaLagConfiguration {

    @Bean(destroyMethod = "close")
    AdminClient flowforgeLagAdminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    KafkaLagQuery kafkaLagQuery(
            @Qualifier("flowforgeLagAdminClient") AdminClient admin,
            @Value("${flowforge.execution.dispatch.topic:flowforge.execution.dispatch.v1}")
            String dispatchTopic,
            @Value("${flowforge.worker.consumer.dlt-topic:flowforge.execution.dispatch.v1.DLT}")
            String dltTopic,
            @Value("${flowforge.worker.consumer.group-id:flowforge-workers}")
            String groupId) {
        return new KafkaLagQuery(admin, dispatchTopic, dltTopic, groupId);
    }

    @Bean
    KafkaLagMetrics kafkaLagMetrics(KafkaLagQuery query, Clock clock, MeterRegistry meters) {
        return new KafkaLagMetrics(query, clock, meters);
    }
}
