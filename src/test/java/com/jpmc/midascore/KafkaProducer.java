package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(@Value("${kafka.topic.transactions}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionData) {
        // Each value is now a single number from the comma-separated list
        // We need to process three values at a time
        String[] parts = transactionData.trim().split("\\s*,\\s*");
        if (parts.length >= 3) {
            kafkaTemplate.send(topic, new Transaction(
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                Float.parseFloat(parts[2])
            ));
        }
    }
}