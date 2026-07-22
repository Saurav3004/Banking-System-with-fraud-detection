package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest createAccountRequest){
        log.info("Creating account for: {}",createAccountRequest.getEmail());

        if(accountRepository.existsByEmail(createAccountRequest.getEmail())){
            throw new RuntimeException("Account already exists for email: " + createAccountRequest.getEmail());
        }

        Account account = new Account();
        account.setAccountHolderName(createAccountRequest.getAccountHolderName());
        account.setEmail(createAccountRequest.getEmail());
        account.setPhone(createAccountRequest.getPhone());
        account.setAccountType(createAccountRequest.getAccountType());
        account.setAccountStatus(AccountStatus.ACTIVE);
        account.setBalance(createAccountRequest.getInitialDeposit());
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(
                createAccountRequest.getAccountType() == AccountType.SAVINGS
                ? new BigDecimal("100000")
                : new BigDecimal("500000")
        );

        Account savedAccount = accountRepository.save(account);
        log.info("Account created successfully: {}" , savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);

    }

    /**
     * Get account by account number
     * @param accountNumber
     * @return AccountResponse
     */

    public AccountResponse getAccount(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));

        return mapToResponse(account);
    }

    /**
     * Get account balance by account number
     * @param accountNumber
     * @return
     */

    public BigDecimal getBalance(String accountNumber){
        Account account =
                accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account " +
                        "not found"));

        return account.getBalance();

    }

    /**
     * Block account --> called by Fraud detection service via kafka
     * @param accountNumber
     */

    public void blockAccount(String accountNumber){
        log.info("Blocking the account: {}",accountNumber);
        Account account =
                accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account " +
                        "not found"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account has been blocked: {}",accountNumber);
    }

    /**
     * Deduct balance
     * @param accountNumber
     * @param amount
     */

    public void deductBalance(String accountNumber,BigDecimal amount){
        log.info("Deducting balance {} from account: {}",amount,accountNumber);
        Account account =
                accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account " +
                        "not found"));

       if(account.getAccountStatus() != AccountStatus.ACTIVE){
           throw new RuntimeException("Account is not active" + accountNumber);
       }

       if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient funds for account" + accountNumber);
       }

       account.setBalance(account.getBalance().subtract(amount));
       accountRepository.save(account);

       log.info("Balance updated successfully, New balance {}", account.getBalance());
    }

    /**
     * Credit balance
     * Called by transaction via kafka
     * @param accountNumber
     * @param amount
     */

    public void creditBalance(String accountNumber,BigDecimal amount){
        log.info("Amount credited {} to account {}",amount,accountNumber);
        Account account =
                accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account " +
                        "not found"));

        if(account.getAccountStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("Account is not active" + accountNumber);
        }

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Amount credit successfully, New balance {}",account.getBalance());

    }


//    Generate unique 12 digit account number
    private String generateAccountNumber(){
        String accountNumber;

        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);

            accountNumber = String.format("%012d",number);
        }while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account){
        AccountResponse accountResponse = new AccountResponse();

        accountResponse.setId(account.getId());
        accountResponse.setEmail(account.getEmail());
        accountResponse.setAccountNumber(account.getAccountNumber());
        accountResponse.setAccountHolderName(account.getAccountHolderName());
        accountResponse.setAccountType(account.getAccountType());
        accountResponse.setBalance(account.getBalance());
        accountResponse.setPhone(account.getPhone());
        accountResponse.setAccountStatus(account.getAccountStatus());
        accountResponse.setDailyTransactionLimit(account.getDailyTransactionLimit());
        accountResponse.setCreatedAt(account.getCreatedAt());

        return accountResponse;

    }
}
