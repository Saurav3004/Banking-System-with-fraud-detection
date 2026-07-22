package com.banking.frauddetectionservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionCleanEventDto {
    private String transactionId;
    private boolean fraud;
    private String reason;
}
