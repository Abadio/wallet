package com.recargapay.wallet.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication(exclude = FlywayAutoConfiguration.class)
@EnableKafka
public class WalletConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(WalletConsumerApplication.class, args);
    }
}
