package com.recargapay.wallet.query.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CountDownLatch;

@Component
@ActiveProfiles("integration")
public class DltKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DltKafkaConsumer.class);
    private static CountDownLatch testLatch = new CountDownLatch(1);
    private static ConsumerRecord<String, Object> lastConsumedRecord;

    @PostConstruct
    public void init() {
        logger.info("DltKafkaConsumer initialized");
    }

    public static void setTestLatch(CountDownLatch latch) {
        testLatch = latch;
    }

    public static CountDownLatch getTestLatch() {
        return testLatch;
    }

    public static ConsumerRecord<String, Object> getLastConsumedRecord() {
        return lastConsumedRecord;
    }

    @KafkaListener(
            topics = "wallet-query-dlt",
            groupId = "${kafka.consumer.dlt.group-id}",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consumeEvent(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        logger.info("KafkaListener invoked for topic={}, key={}, value type={}, headers={}",
                record.topic(), record.key(),
                record.value() != null ? record.value().getClass().getName() : "null",
                record.headers());
        try {
            lastConsumedRecord = record;
            logger.info("Stored DLT record: key={}, value={}", record.key(), record.value());
            testLatch.countDown();
            acknowledgment.acknowledge();
            logger.info("Message acknowledged for topic={}, key={}", record.topic(), record.key());
        } catch (Exception e) {
            logger.error("Error processing DLT message: topic={}, key={}, exception={}",
                    record.topic(), record.key(), e.getMessage(), e);
        }
    }
}