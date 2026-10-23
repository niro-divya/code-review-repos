package com.example.lending.risk.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Produces on broker-a:
 *  - "risk.assessed" -> consumed by loan-service (TRUE shared event)
 *  - "loan.rejected" -> ORPHAN: no consumer anywhere (orphan-event trap)
 */
@Component
public class RiskEventProducer {

    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public RiskEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishRiskAssessed(RiskAssessedEvent event) {
        try {
            kafkaTemplate.send("risk.assessed", String.valueOf(event.getLoanId()),
                    event.getDecision() + ":" + event.getScore())
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publishing risk.assessed for loan " + event.getLoanId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish risk.assessed for loan " + event.getLoanId(), e);
        }
    }

    public void publishLoanRejected(Long loanId) {
        kafkaTemplate.send("loan.rejected", String.valueOf(loanId), "REJECTED");
    }
}
