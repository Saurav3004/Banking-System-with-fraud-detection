package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

    /**
     * SAGA Step - 1: Initiate transfer
     * Deducts amount from sender via feign client
     * save transaction as PROCESSING
     * publish event to kafka for fraud-check
     * Returns
     * @param transferRequest
     * @return
     */
    public TransactionResponse transfer(TransferRequest transferRequest){

        log.info("SAGA START - transfer: {} -> {} amount: {}",
                transferRequest.getSenderAccountNumber(),
                transferRequest.getReceiverAccountNumber(),
                transferRequest.getAmount()
                );

//        SAGA Step - 1 => Deduct from sender
        accountServiceClient.deductBalance(transferRequest.getSenderAccountNumber(),transferRequest.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(transferRequest.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(transferRequest.getReceiverAccountNumber());
        transaction.setAmount(transferRequest.getAmount());
        transaction.setDescription(transferRequest.getDescription());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction transactionSaved = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}",transactionSaved.getId());

        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                transactionSaved.getId(),
                transactionSaved.getSenderAccountNumber(),
                transactionSaved.getReceiverAccountNumber(),
                transactionSaved.getAmount(),
                transactionSaved.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,transactionSaved.getId(),event);
        log.info("SAGA Step - 2 TransactionInitiated event published: {}",transactionSaved.getId());

        return  mapToResponse(transactionSaved);
    }

    public TransactionResponse getTransaction(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found " + transactionId));
        return mapToResponse(transaction);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber){
        List<Transaction> transactions = transactionRepository.findBySenderAccountNumberOrderByCreatedDesc(accountNumber);
        return transactions.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    private TransactionResponse mapToResponse(Transaction transaction){
        TransactionResponse transactionResponse = new TransactionResponse();
        transactionResponse.setId(transaction.getId());
        transactionResponse.setAmount(transaction.getAmount());
        transactionResponse.setDescription(transaction.getDescription());
        transactionResponse.setReferenceNumber(transaction.getReferenceNumber());
        transactionResponse.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        transactionResponse.setStatus(transaction.getStatus());
        transactionResponse.setTransactionType(transaction.getType());
        transactionResponse.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionResponse.setFailureReason(transaction.getFailureReason());
        transactionResponse.setCreatedAt(transaction.getCreatedAt());
        transactionResponse.setCompletedAt(transaction.getCompletedAt());

        return transactionResponse;
    }
}
