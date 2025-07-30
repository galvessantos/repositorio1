package com.montreal.msiav_bh.service;

import com.montreal.msiav_bh.config.ApiQueryConfig;
import com.montreal.msiav_bh.dto.request.ConsultaAuthRequestDTO;
import com.montreal.msiav_bh.dto.request.ConsultaSearchRequestDTO;
import com.montreal.msiav_bh.dto.response.ConsultaAuthResponseDTO;
import com.montreal.msiav_bh.dto.response.ConsultaNotificationResponseDTO;
import com.montreal.msiav_bh.dto.response.QueryDetailResponseDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ApiQueryService {

    private static final Logger logger = LoggerFactory.getLogger(ApiQueryService.class);

    @Autowired
    private ApiQueryConfig config;
    @Autowired
    private RestTemplate restTemplate;

    private String currentToken;
    private LocalDateTime tokenExpiration;
    private final ReentrantLock tokenLock = new ReentrantLock();

    public String authenticate() {
        tokenLock.lock();
        try {
            if (currentToken != null && tokenExpiration != null &&
                    LocalDateTime.now().isBefore(tokenExpiration.minusMinutes(1))) {
                return currentToken;
            }

            String url = config.getBaseUrl() + "/api/sanctum/token";
            ConsultaAuthRequestDTO request = new ConsultaAuthRequestDTO(
                    config.getUsername(),
                    config.getPassword()
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ResponseEntity<ConsultaAuthResponseDTO> response = restTemplate.postForEntity(
                    url,
                    new HttpEntity<>(request, headers),
                    ConsultaAuthResponseDTO.class
            );

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                currentToken = response.getBody().data().token();
                tokenExpiration = LocalDateTime.now().plusSeconds(config.getTokenRefreshInterval() / 1000);
                return currentToken;
            }
            throw new RuntimeException("Autenticação falhou: " + response.getStatusCode());
        } catch (Exception e) {
            logger.error("Falha crítica na autenticação", e);
            throw new RuntimeException("Erro na autenticação: " + e.getMessage());
        } finally {
            tokenLock.unlock();
        }
    }



    public List<ConsultaNotificationResponseDTO.NotificationData> searchByPeriod(LocalDate startDate, LocalDate endDate) {
        String token = authenticate();
        String url = config.getBaseUrl() + "/api/recepcaoContrato/periodo/"
                + startDate.toString() + "/" + endDate.toString();

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));

        try {
            ResponseEntity<ConsultaNotificationResponseDTO> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    ConsultaNotificationResponseDTO.class
            );

            if (response.getStatusCode() == HttpStatus.OK &&
                    response.getBody() != null &&
                    response.getBody().success()) {
                return response.getBody().data();
            }
            logger.warn("Resposta inesperada: {}", response.getStatusCode());
        } catch (HttpClientErrorException e) {
            logger.error("Erro na requisição: {} - {}", e.getStatusCode(), e.getResponseBodyAsString());
        } catch (Exception e) {
            logger.error("Erro inesperado: ", e);
        }
        return List.of();
    }

    public QueryDetailResponseDTO searchContract(String contractNumber) {
        String url = config.getBaseUrl() + "/api/recepcaoContrato/receber";
        String token = authenticate();

        Map<String, String> body = new HashMap<>();
        body.put("nu_contrato", contractNumber);


        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.setContentType(MediaType.APPLICATION_JSON);


        HttpEntity<Map<String, String>> requestEntity = new HttpEntity<>(body, headers);


        ResponseEntity<QueryDetailResponseDTO> response =
                restTemplate.postForEntity(url, requestEntity, QueryDetailResponseDTO.class);

        return response.getBody();
    }

    public List<ConsultaNotificationResponseDTO.NotificationData> searchCancelledByPeriod(LocalDate startDate, LocalDate endDate) {
        String token = authenticate();
        String url = config.getBaseUrl() + "/api/recepcaoContrato/cancelados/periodo/" + startDate + "/" + endDate;

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            ResponseEntity<ConsultaNotificationResponseDTO> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, ConsultaNotificationResponseDTO.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null && response.getBody().success()) {
                return response.getBody().data();
            }
            return List.of();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                currentToken = null;
                return searchCancelledByPeriod(startDate, endDate);
            }
            logger.error("Error searching cancelled", e);
            throw new RuntimeException("Search cancelled failed: " + e.getMessage());
        }
    }

    public String testConnection() {
        try {
            String testUrl = config.getBaseUrl();
            ResponseEntity<String> pingResponse = restTemplate.getForEntity(testUrl, String.class);

            if (!pingResponse.getStatusCode().is2xxSuccessful()) {
                return "Falha ao acessar URL base: " + pingResponse.getStatusCode();
            }

            String authUrl = config.getBaseUrl() + "/api/sanctum/token";
            ConsultaAuthRequestDTO request = new ConsultaAuthRequestDTO(
                    config.getUsername(),
                    config.getPassword()
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ResponseEntity<String> authResponse = restTemplate.postForEntity(
                    authUrl,
                    new HttpEntity<>(request, headers),
                    String.class
            );

            return "Conexão OK! Status: " + authResponse.getStatusCode()
                    + "\nResposta: " + authResponse.getBody();
        } catch (Exception e) {
            return "Falha na conexão:\nURL: " + config.getBaseUrl()
                    + "\nErro: " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }
}