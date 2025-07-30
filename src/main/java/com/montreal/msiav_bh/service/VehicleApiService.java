package com.montreal.msiav_bh.service;

import com.montreal.msiav_bh.dto.PageDTO;
import com.montreal.msiav_bh.dto.VehicleDTO;
import com.montreal.msiav_bh.dto.response.ConsultaNotificationResponseDTO;
import com.montreal.msiav_bh.dto.response.QueryDetailResponseDTO;
import com.montreal.msiav_bh.mapper.VehicleInquiryMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehicleApiService {

    private final ApiQueryService apiQueryService;
    private final VehicleInquiryMapper vehicleInquiryMapper;
    private final VehicleCacheService vehicleCacheService;

    private static final String CIRCUIT_BREAKER_NAME = "vehicle-api";
    private static final int API_TIMEOUT_SECONDS = 30;

    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "fallbackGetVehicles")
    @Retry(name = CIRCUIT_BREAKER_NAME)
    public PageDTO<VehicleDTO> getVehiclesWithFallback(
            LocalDate dataInicio, LocalDate dataFim, String credor, String contrato,
            String protocolo, String cpf, String uf, String cidade, String modelo,
            String placa, String etapaAtual, String statusApreensao,
            int page, int size, String sortBy, String sortDir) {

        CompletableFuture<PageDTO<VehicleDTO>> future = CompletableFuture.supplyAsync(() ->
                fetchFromApi(dataInicio, dataFim, credor, contrato, protocolo, cpf,
                        uf, cidade, modelo, placa, etapaAtual, statusApreensao,
                        page, size, sortBy, sortDir)
        );
        try {
            PageDTO<VehicleDTO> result = future.get(API_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            CompletableFuture.runAsync(() -> vehicleCacheService.updateCache(result.content()));
            return result;
        } catch (Exception e) {
            throw new RuntimeException("API call failed", e);
        }
    }

    public PageDTO<VehicleDTO> fallbackGetVehicles(
            LocalDate dataInicio, LocalDate dataFim, String credor, String contrato,
            String protocolo, String cpf, String uf, String cidade, String modelo,
            String placa, String etapaAtual, String statusApreensao,
            int page, int size, String sortBy, String sortDir, Exception ex) {

        Sort.Direction direction = "desc".equalsIgnoreCase(sortDir) ? Sort.Direction.DESC : Sort.Direction.ASC;
        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, sortBy));
        Page<VehicleDTO> cached = vehicleCacheService.getFromCache(
                dataInicio, dataFim, credor, contrato, protocolo, cpf,
                uf, cidade, modelo, placa, etapaAtual, statusApreensao, pageable
        );
        return PageDTO.of(
                cached.getContent(),
                cached.getNumber(),
                cached.getSize(),
                cached.getTotalElements()
        );
    }

    private PageDTO<VehicleDTO> fetchFromApi(
            LocalDate dataInicio, LocalDate dataFim, String credor, String contrato,
            String protocolo, String cpf, String uf, String cidade, String modelo,
            String placa, String etapaAtual, String statusApreensao,
            int page, int size, String sortBy, String sortDir) {

        List<ConsultaNotificationResponseDTO.NotificationData> notifications =
                (dataInicio != null && dataFim != null)
                        ? apiQueryService.searchByPeriod(dataInicio, dataFim)
                        : apiQueryService.searchByPeriod(LocalDate.now().minusDays(30), LocalDate.now());

        List<VehicleDTO> vehicles = vehicleInquiryMapper.mapToVeiculoDTO(notifications);
        vehicles = vehicles.stream()
                .filter(v -> credor == null || v.credor().contains(credor))
                .filter(v -> contrato == null || v.contrato().equals(contrato))
                .filter(v -> protocolo == null || v.protocolo().equals(protocolo))
                .filter(v -> cpf == null || v.cpfDevedor().equals(cpf))
                .filter(v -> uf == null || v.uf().equals(uf))
                .filter(v -> cidade == null || v.cidade().contains(cidade))
                .filter(v -> modelo == null || v.modelo().contains(modelo))
                .filter(v -> placa == null || v.placa().equals(placa))
                .filter(v -> etapaAtual == null || v.etapaAtual().equals(etapaAtual))
                .filter(v -> statusApreensao == null || v.statusApreensao().equals(statusApreensao))
                .toList();

        vehicles = sort(vehicles, sortBy, sortDir);
        return paginate(vehicles, page, size);
    }

    private List<VehicleDTO> sort(List<VehicleDTO> vehicles, String sortBy, String sortDir) {
        return vehicles;
    }

    private PageDTO<VehicleDTO> paginate(List<VehicleDTO> vehicles, int page, int size) {
        int total = vehicles.size();
        int start = page * size;
        int end = Math.min(start + size, total);
        return start >= total
                ? PageDTO.of(List.of(), page, size, total)
                : PageDTO.of(vehicles.subList(start, end), page, size, total);
    }

    public QueryDetailResponseDTO searchContract(String contrato) {
        return apiQueryService.searchContract(contrato);
    }
}