package com.banking.transactionservice.service;


import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.event.SendOtpEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;
    private final RedisTemplate<String,String> redisTemplate;
    private static final long OTP_EXPIRY_MINUTES = 5;
    private final KafkaTemplate<String,Object> kafkaTemplate;


    private static final String TRANSACTION_OTP_GENERATED_TOPIC="transaction.otp.generated";

    @KafkaListener(topics = "verification.required",groupId = "transaction-service-group")
    public void consumeVerificationRequired(@Payload Map<String,Object> payload){
        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transaction: {} reason - {}",transactionId,reason);

            Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found" + transactionId));

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction {} not processing - skipping", transactionId);
                return;
            }

            String otp = String.format("%06d",(int) (Math.random() * 900000) + 100000);
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey,otp, Duration.ofMinutes(OTP_EXPIRY_MINUTES));

            // Update status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction: {} expires in {} min",transactionId,OTP_EXPIRY_MINUTES);

            //Notify user
            SendOtpEvent otpEvent = new SendOtpEvent();
            otpEvent.setTransactionId(transactionId);
            otpEvent.setAccountNumber(accountNumber);
            otpEvent.setAmount(payload.get("amount").toString());
            otpEvent.setReason(reason);
            otpEvent.setOtp(otp);

            // This event consumed by notification service
            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC,transactionId,otpEvent);

        } catch (Exception e) {
            log.error("error handling verification required: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(@Payload Map<String,Object> payload){
        try{
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);
        } catch (Exception e) {
            log.error("Error processing fraud check result: {}",e.getMessage());
        }
    }
}
