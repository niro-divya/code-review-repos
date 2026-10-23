package com.example.lending.loan.client;

import com.example.lending.loan.dto.RiskAssessmentDto;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

/**
 * CROSS-REPO HTTP call (WebClient): loan-service -> risk-service GET /api/v1/risk/{id}.
 * Same target service as RiskClient (Feign) but a different client library and endpoint.
 */
@Component
public class RiskWebClient {

    private static final Duration RISK_CALL_TIMEOUT = Duration.ofSeconds(5);

    private final WebClient webClient;

    public RiskWebClient(WebClient riskWebClient) {
        this.webClient = riskWebClient;
    }

    public RiskAssessmentDto getRisk(Long id) {
        return webClient.get()
                .uri("/api/v1/risk/{id}", id)
                .retrieve()
                .bodyToMono(RiskAssessmentDto.class)
                .block(RISK_CALL_TIMEOUT);
    }
}
