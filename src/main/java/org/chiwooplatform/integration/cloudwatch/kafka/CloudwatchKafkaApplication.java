package org.chiwooplatform.integration.cloudwatch.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import org.chiwooplatform.integration.cloudwatch.kafka.support.TransactionLoggingFilter;

@SpringBootApplication
public class CloudwatchKafkaApplication {

    public static final String TXID = "TXID";

    public static void main( String[] args ) {
        SpringApplication.run( CloudwatchKafkaApplication.class, args );
    }

    @Bean
    public TransactionLoggingFilter transactionLoggingFilter() {
        return new TransactionLoggingFilter();
    }
}
