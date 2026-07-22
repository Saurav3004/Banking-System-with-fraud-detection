package com.banking.transactionservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FraudEvent {
    private String transactionId;
    private String senderAccountNumber;
    private String reason;
}
