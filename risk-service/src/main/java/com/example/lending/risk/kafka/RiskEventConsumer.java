package com.example.lending.risk.kafka;

import com.example.lending.risk.dto.LoanDto;
import com.example.lending.risk.dto.RiskAssessmentDto;
import com.example.lending.risk.service.RiskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * TRUE shared event: consumes "loan.applied" from broker-a (produced by loan-service on broker-a).
 * After assessing, emits risk.assessed via RiskEventProducer.
 */
@Component
public class RiskEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RiskEventConsumer.class);

    private final RiskService riskService;
    private final RiskEventProducer producer;

    public RiskEventConsumer(RiskService riskService, RiskEventProducer producer) {
        this.riskService = riskService;
        this.producer = producer;
    }

    @KafkaListener(topics = "loan.applied", groupId = "risk")
    public void onLoanApplied(LoanAppliedEvent event) {
        if (event == null || event.getLoanId() == null) {
            log.warn("Skipping loan.applied event without a loanId");
            return;
        }
        LoanDto loan = new LoanDto(event.getLoanId(), event.getAmount(), "STANDARD", event.getUserId());
        RiskAssessmentDto assessment = riskService.assessRisk(loan);
        producer.publishRiskAssessed(new RiskAssessedEvent(
                assessment.getLoanId(), assessment.getScore(), assessment.getDecision()));
        if ("REJECT".equals(assessment.getDecision())) {
            producer.publishLoanRejected(event.getLoanId());
        }
    }
}
