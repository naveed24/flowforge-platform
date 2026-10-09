package com.flowforge.execution;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ExecutionClockConfiguration {

    @Bean
    public Clock executionClock() {
        return Clock.systemUTC();
    }
}
