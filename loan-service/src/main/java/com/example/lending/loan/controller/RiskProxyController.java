package com.example.lending.loan.controller;

import com.example.lending.loan.client.RiskWebClient;
import com.example.lending.loan.dto.RiskAssessmentDto;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * SERVER-SIDE PROXY (Java): GET /api/v1/risk-proxy/{id} forwards to
 * risk-service GET /api/v1/risk/{id} via RiskWebClient. A cross-repo proxy edge that
 * lives inside a Java service (not just the BFF).
 */
@RestController
@RequestMapping("/api/v1/risk-proxy")
public class RiskProxyController {

    private final RiskWebClient riskWebClient;

    public RiskProxyController(RiskWebClient riskWebClient) {
        this.riskWebClient = riskWebClient;
    }

    @GetMapping("/{id}")
    public RiskAssessmentDto proxyRisk(@PathVariable Long id) {
        RiskAssessmentDto risk = riskWebClient.getRisk(id);
        if (risk == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no risk assessment for loan " + id);
        }
        return risk;
    }
}
