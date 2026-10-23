package com.flowforge.execution;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Fail closed: never silently advance past a record whose database write
 * failed or whose payload is invalid. Dead-letter handling is a later slice.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
@ConditionalOnProperty(prefix = "flowforge.worker.consumer",
        name = "enabled", havingValue = "true")
public class WorkerKafkaErrorConfiguration {

    @Bean
    DefaultErrorHandler workerDispatchErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(
                5000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
