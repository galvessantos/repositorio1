package com.montreal.msiav_bh.service;

import com.montreal.msiav_bh.context.CacheUpdateContext;
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

            CompletableFuture.runAsync(() -> {
                boolean hasSpecificFilters = credor != null || contrato != null || protocolo != null ||
                        cpf != null || uf != null || cidade != null ||
                        modelo != null || placa != null || etapaAtual != null ||
                        statusApreensao != null;

                boolean hasCustomDateRange = false;
                if (dataInicio != null && dataFim != null) {
                    LocalDate defaultStart = LocalDate.now().minusDays(30);
                    LocalDate defaultEnd = LocalDate.now();
                    hasCustomDateRange = !dataInicio.equals(defaultStart) || !dataFim.equals(defaultEnd);
                }

                CacheUpdateContext context = (hasSpecificFilters || hasCustomDateRange)
                        ? CacheUpdateContext.filteredSearch(dataInicio, dataFim, credor, contrato, protocolo, cpf, uf, cidade, modelo, placa, etapaAtual, statusApreensao)
                        : CacheUpdateContext.fullRefresh();

                vehicleCacheService.updateCache(result.content(), context);
            });

            return result;
        } catch (Exception e) {
            throw new RuntimeException("API call failed", e);
        }
    }

    public PageDTO<VehicleDTO> fallbackGetVehicles(
            LocalDate dataInicio, LocalDate dataFim,
            String credor, String contrato,
            String protocolo, String cpf,
            String uf, String cidade,
            String modelo, String placa,
            String etapaAtual, String statusApreensao,
            int page, int size,
            String sortBy, String sortDir,
            Throwable throwable) {

        log.warn("API externa falhou, usando cache como fallback. Erro: {}", throwable.getMessage());

        if (!vehicleCacheService.isCacheValid()) {
            log.error("Cache is empty or invalid - cannot provide fallback");
            return PageDTO.of(List.of(), page, size, 0);
        }

        Sort.Direction direction = "desc".equalsIgnoreCase(sortDir) ? Sort.Direction.DESC : Sort.Direction.ASC;
        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, "id"));

        Page<VehicleDTO> cached = vehicleCacheService.getFromCache(
                dataInicio, dataFim, credor, contrato, protocolo, cpf,
                uf, cidade, modelo, placa, etapaAtual, statusApreensao, pageable
        );

        log.info("Fallback returned {} results from cache", cached.getTotalElements());

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

        LocalDate searchStart = dataInicio;
        LocalDate searchEnd = dataFim;

        if (dataInicio == null || dataFim == null) {
            searchStart = LocalDate.now().minusDays(30);
            searchEnd = LocalDate.now();
        }

        List<ConsultaNotificationResponseDTO.NotificationData> notifications =
                apiQueryService.searchByPeriod(searchStart, searchEnd);

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

        return paginate(vehicles, page, size);
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

    private boolean isDefaultDateRange(LocalDate dataInicio, LocalDate dataFim) {
        LocalDate defaultStart = LocalDate.now().minusDays(30);
        LocalDate defaultEnd = LocalDate.now();

        return dataInicio.equals(defaultStart) &&
                (dataFim.equals(defaultEnd) || dataFim.equals(defaultEnd.minusDays(1)));
    }
}