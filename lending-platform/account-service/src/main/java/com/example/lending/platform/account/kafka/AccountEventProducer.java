package com.example.lending.platform.account.kafka;

import com.example.lending.platform.common.event.AccountCreatedEvent;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Produces "account.created" on broker-a (TRUE shared: notification-worker consumes it).
 * Uses the SHARED AccountCreatedEvent from platform-common.
 */
@Component
public class AccountEventProducer {

    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public AccountEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishAccountCreated(AccountCreatedEvent event) {
        try {
            kafkaTemplate.send("account.created", String.valueOf(event.getAccountId()), event.getName())
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publishing account.created for account " + event.getAccountId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish account.created for account " + event.getAccountId(), e);
        }
    }
}
