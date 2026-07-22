package com.banking.transactionservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RefundEvent {
    private String transactionId;
    private BigDecimal amount;
    private String senderAccountNumber;
    private String reason;
}

