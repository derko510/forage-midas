package com.jpmc.midascore.foundation;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.service.IncentiveService;

@Component
public class TransactionListener {

    @Value("${kafka.topic.transactions}")
    private String topic;

    @PersistenceContext
    private EntityManager entityManager;

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveService incentiveService;

    public TransactionListener(
            UserRepository userRepository, 
            TransactionRecordRepository transactionRecordRepository,
            IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${kafka.topic.transactions}", groupId = "midas-core-group")
    @Transactional
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Validate transaction
        if (sender == null || recipient == null || sender.getBalance() < transaction.getAmount()) {
            return; // Discard invalid transaction
        }

        // Get incentive from API
        float incentive = incentiveService.getIncentive(transaction).getAmount();

        // Create and save transaction record
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive);
        transactionRecordRepository.save(record);

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive);
        
        userRepository.save(sender);
        userRepository.save(recipient);
    }
} 