package com.banking.transactionservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SendOtpEvent {
    private String transactionId;
    private String accountNumber;
    private String reason;
    private String otp;
    private String amount;
}
