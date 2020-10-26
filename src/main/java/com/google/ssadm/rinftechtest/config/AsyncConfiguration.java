package com.google.ssadm.rinftechtest.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
The @EnableAsync annotation enables Spring's ability to run  @Async methods in a
background thread pool. The bean executor helps to customize the thread
executor such as configuring the number of threads for an application, queue limit
size, and so on. Spring will specifically look for this bean when the server is
started. If this bean is not defined, Spring will create SimpleAsyncTaskExecutor by default.
 */

@Configuration
@EnableAsync
public class AsyncConfiguration {

    private static final Logger log = LoggerFactory.getLogger(AsyncConfiguration.class);

    @Bean(name = "executor")
    public Executor taskExecutor() {
        log.debug("Creating async task executor");
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("Processor-");
        executor.initialize();
        return executor;
    }
}
