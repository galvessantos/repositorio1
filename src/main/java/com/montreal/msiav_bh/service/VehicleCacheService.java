package com.montreal.msiav_bh.service;

import com.montreal.msiav_bh.context.CacheUpdateContext;
import com.montreal.msiav_bh.dto.VehicleDTO;
import com.montreal.msiav_bh.entity.VehicleCache;
import com.montreal.msiav_bh.mapper.VehicleCacheMapper;
import com.montreal.msiav_bh.repository.VehicleCacheRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehicleCacheService {

    private final VehicleCacheRepository vehicleCacheRepository;
    private final VehicleCacheMapper vehicleCacheMapper;

    @Value("${vehicle.cache.expiry.minutes:15}")
    private int cacheExpiryMinutes;

    @Value("${vehicle.cache.retention.days:7}")
    private int cacheRetentionDays;

    public boolean isCacheValid() {
        Optional<LocalDateTime> lastSyncOpt = vehicleCacheRepository.findLastSyncDate();

        if (lastSyncOpt.isEmpty()) {
            log.info("DEBUG: No sync date found in cache");
            return false;
        }

        LocalDateTime lastSync = lastSyncOpt.get();
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(cacheExpiryMinutes);
        boolean isValid = lastSync.isAfter(cutoff);

        log.info("DEBUG: Last sync: {}, Cutoff: {}, Valid: {}", lastSync, cutoff, isValid);

        return isValid;
    }

    @Transactional
    public void updateCache(List<VehicleDTO> vehicles) {
        CacheUpdateContext context = CacheUpdateContext.fullRefresh();
        updateCache(vehicles, context);
    }

    @Transactional
    public void updateCache(List<VehicleDTO> vehicles, CacheUpdateContext context) {
        log.info("Updating vehicle cache with {} vehicles. Context: {}", vehicles.size(), context);

        try {
            LocalDateTime syncDate = LocalDateTime.now();

            if (context.isFullRefresh()) {
                handleFullRefresh(vehicles, syncDate, context);
            } else {
                handleIncrementalUpdate(vehicles, syncDate, context);
            }

            cleanOldCache();
            log.info("Vehicle cache updated successfully");
        } catch (Exception e) {
            log.error("Error updating vehicle cache", e);
            throw new RuntimeException("Failed to update vehicle cache", e);
        }
    }

    private void handleFullRefresh(List<VehicleDTO> vehicles, LocalDateTime syncDate, CacheUpdateContext context) {
        if (vehicles.isEmpty() && !context.isHasFilters()) {
            log.warn("API returned empty without filters - clearing entire cache");
            vehicleCacheRepository.deleteAll();
            return;
        }

        if (vehicles.isEmpty()) {
            log.info("API returned empty with filters - preserving existing cache");
            return;
        }

        Set<String> activePlacas = vehicles.stream()
                .map(VehicleDTO::placa)
                .filter(Objects::nonNull)
                .filter(placa -> !"N/A".equals(placa) && !placa.trim().isEmpty())
                .collect(Collectors.toSet());

        if (!activePlacas.isEmpty()) {
            int removedCount = vehicleCacheRepository.countByPlacaNotIn(activePlacas);
            vehicleCacheRepository.deleteByPlacaNotIn(activePlacas);
            log.info("Removed {} vehicles no longer in API. Active placas: {}", removedCount, activePlacas.size());
        } else {
            log.warn("No valid placas found in API response - skipping cleanup to prevent data loss");
        }

        updateOrInsertVehicles(vehicles, syncDate);
    }

    private void handleIncrementalUpdate(List<VehicleDTO> vehicles, LocalDateTime syncDate, CacheUpdateContext context) {
        updateOrInsertVehicles(vehicles, syncDate);
        log.info("Incremental update completed for {} vehicles", vehicles.size());
    }

    private void updateOrInsertVehicles(List<VehicleDTO> vehicles, LocalDateTime syncDate) {
        for (VehicleDTO dto : vehicles) {
            Optional<VehicleCache> existing = vehicleCacheRepository.findByContrato(dto.contrato());

            if (existing.isEmpty() && dto.protocolo() != null && !"N/A".equals(dto.protocolo())) {
                existing = vehicleCacheRepository.findByProtocolo(dto.protocolo());
            }

            if (existing.isEmpty() && dto.placa() != null && !"N/A".equals(dto.placa())) {
                existing = vehicleCacheRepository.findByPlaca(dto.placa());
            }

            if (existing.isPresent()) {
                VehicleCache updated = updateExistingVehicle(existing.get(), dto, syncDate);
                vehicleCacheRepository.save(updated);
                log.debug("Updated existing vehicle: {}", dto.contrato());
            } else {
                VehicleCache newEntity = vehicleCacheMapper.toEntity(dto, syncDate);
                vehicleCacheRepository.save(newEntity);
                log.debug("Inserted new vehicle: {}", dto.contrato());
            }
        }
    }

    private VehicleCache updateExistingVehicle(VehicleCache existing, VehicleDTO dto, LocalDateTime syncDate) {
        existing.setCredor(dto.credor());
        existing.setDataPedido(dto.dataPedido());
        existing.setContrato(dto.contrato());
        existing.setPlaca(dto.placa());
        existing.setModelo(dto.modelo());
        existing.setUf(dto.uf());
        existing.setCidade(dto.cidade());
        existing.setCpfDevedor(dto.cpfDevedor());
        existing.setProtocolo(dto.protocolo());
        existing.setEtapaAtual(dto.etapaAtual());
        existing.setStatusApreensao(dto.statusApreensao());
        existing.setUltimaMovimentacao(dto.ultimaMovimentacao());
        existing.setApiSyncDate(syncDate);
        return existing;
    }

    public Page<VehicleDTO> getFromCache(LocalDate dataInicio, LocalDate dataFim,
                                         String credor, String contrato,
                                         String protocolo, String cpf,
                                         String uf, String cidade,
                                         String modelo, String placa,
                                         String etapaAtual, String statusApreensao,
                                         Pageable pageable) {

        log.info("Retrieving vehicles from cache");

        Page<VehicleCache> cachedVehicles = vehicleCacheRepository.findWithFiltersFixed(
                dataInicio, dataFim, credor, contrato, protocolo, cpf,
                uf, cidade, modelo, placa, etapaAtual, statusApreensao, pageable
        );

        return cachedVehicles.map(vehicleCacheMapper::toDTO);
    }

    public Page<VehicleDTO> getAllFromCache(Pageable pageable) {
        log.info("Retrieving all vehicles from cache");
        Page<VehicleCache> cachedVehicles = vehicleCacheRepository.findLatestCachedVehicles(pageable);
        return cachedVehicles.map(vehicleCacheMapper::toDTO);
    }

    @Transactional
    public void cleanOldCache() {
        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(cacheRetentionDays);
        vehicleCacheRepository.deleteOldCacheEntries(cutoffDate);
        log.info("Cleaned cache entries older than {}", cutoffDate);
    }

    @Transactional
    public void invalidateCache() {
        log.info("Invalidating entire vehicle cache");
        vehicleCacheRepository.deleteAll();
    }
}