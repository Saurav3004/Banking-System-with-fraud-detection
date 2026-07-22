package com.banking.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated",groupId = "notification-service-group")
    public void consumeOtpGenerated(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,"TRANSACTION VERIFICATION REQUIRED",String.format("Suspicious activity detected on your " +
                    "account"+
                    "Reason: %s"+ "A transaction of %s is pending verification"+ "Your OTP is: %s.Valid for 5 minutes."+
                    "If this wasn't you - ignore this message.",reason,amount,otp));

        }catch (Exception e){
            log.error("Error sending OTP notification: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){

        try{
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            // DEBIT ALERT
            sendAlert(senderAccountNumber,"DEBIT ALERT",String.format("%s debited from account %s",amount,senderAccountNumber));

            // CREDIT ALERT
            sendAlert(receiverAccountNumber,"CREDIT ALERT",String.format("%s credit to account %s",amount,
                    receiverAccountNumber));
        } catch (Exception e) {
            log.error("Error sending transaction notification: {}",e.getMessage());
        }

    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){
        try{
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(senderAccountNumber,"SUSPICIOUS ACTIVITY DETECTED",String.format("Your account has been " +
                    "blocked" + "Reason: %s. "+ "Please contact your bank immediately",reason));

        }catch (Exception e){

            log.error("Error sending fraud alert notification: {}",e.getMessage());

        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void transactionRefunded(@Payload Map<String,Object> payload){
        try{
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");
            sendAlert(senderAccountNumber,"REFUND PROCESSED",String.format("Your transaction of %s was cancelled." +
                            "Reason: %s " +
                            "%s has been refunded to account %s",
                    amount,reason,amount,senderAccountNumber));

        } catch (Exception e) {
            log.error("Error sending refund notification: {}",e.getMessage());        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String,Object> payload){
        try{
            String senderAccountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String razorpayId = (String) payload.get("razorpayPaymentId");

            sendAlert(senderAccountNumber,"PAYMENT SUCCESSFUL",String.format("Payment of %s completed. " +
                    "Razorpay id: %s",amount,razorpayId));
        } catch (Exception e) {
            log.error("Error sending payment completed notification: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String,Object> payload){
        try{
            String senderAccountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(senderAccountNumber,"PAYMENT FAILED",String.format("Payment of %s could not be processed. " +
                    "Please try again or contact support", amount));
        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}",e.getMessage());
        }
    }

    private void sendAlert(String accountNumber,String subject,String message){
        log.info("-------------------------------------------");
        log.info("Account: {}",accountNumber);
        log.info("Subject: {}",subject);
        log.info("Message: {}",message);
        log.info("-------------------------------------------");
    }
}
