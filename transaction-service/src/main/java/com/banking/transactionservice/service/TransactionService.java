package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.event.FraudEvent;
import com.banking.transactionservice.event.RefundEvent;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
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
    private final RedisTemplate<String,String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

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
        List<Transaction> transactions = transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber);
        return transactions.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId,String OTP){
        log.info("OTP verification for this transaction: {}",transactionId);

        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found"+ transactionId));

        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            // OTP has been expired for this transaction
            log.warn("OTP expired for transaction: {}",transactionId);
            compensateTransaction(transaction,"OTP has been expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(OTP)){
//            BLOCK ACCOUNT and REFUND
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction,"Wrong OTP entered - transaction cancelled" + "account blocked" +
                    "for security");

            return mapToResponse(transaction);
        }

        log.info("OTP - verified completing transaction {}",transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);

    }

    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found" + transactionId));
        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not processing - skipping", transactionId);
            return;
        }
        completeTransaction(transaction);
    }

    private void compensateTransaction(Transaction transaction,String reason){
        log.warn("SAGA COMPENSATION refunding: {} amount: {}",transaction.getSenderAccountNumber(),transaction.getAmount());

        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(),transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + "- SAGA compensation executed, amount refunded at " + LocalDateTime.now());

        transactionRepository.save(transaction);

        // Publish refund event - Notification service will alert user
        RefundEvent refundEvent = new RefundEvent();
        refundEvent.setTransactionId(transaction.getId());
        refundEvent.setSenderAccountNumber(transaction.getSenderAccountNumber());
        refundEvent.setAmount(transaction.getAmount());
        refundEvent.setReason(transaction.getFailureReason());

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA compensation complete {} refunded to {}",transaction.getAmount(),
                transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction,String reason){
        // Publish event for fraud.detected
        FraudEvent fraudEvent = new FraudEvent();
        fraudEvent.setTransactionId(transaction.getId());
        fraudEvent.setSenderAccountNumber(transaction.getSenderAccountNumber());
        fraudEvent.setReason(reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getId(),fraudEvent);

        log.warn("Fraud detected published - account: {} will be blocked, Kindly contact to your bank",
                transaction.getSenderAccountNumber());

        // SAGA Compensation - refund to sender
        compensateTransaction(transaction,reason);
    }

    private void completeTransaction(Transaction transaction){
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);
        // Publish event for transaction.completed
        TransactionCompletedEvent transactionCompletedEvent = new TransactionCompletedEvent();
        transactionCompletedEvent.setTransactionId(transaction.getId());
        transactionCompletedEvent.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionCompletedEvent.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        transactionCompletedEvent.setAmount(transaction.getAmount());
        transactionCompletedEvent.setDescription(transaction.getDescription());

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),transactionCompletedEvent);

        log.info("SAGA completed - Transaction: {} completed",transaction.getId());
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
