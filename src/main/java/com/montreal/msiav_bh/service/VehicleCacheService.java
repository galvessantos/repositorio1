package com.montreal.msiav_bh.service;

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
        return vehicleCacheRepository.findLastSyncDate()
                .map(lastSync -> lastSync.isAfter(LocalDateTime.now().minusMinutes(cacheExpiryMinutes)))
                .orElse(false);
    }

    @Transactional
    public void updateCache(List<VehicleDTO> vehicles) {
        log.info("Updating vehicle cache with {} vehicles", vehicles.size());

        try {
            LocalDateTime syncDate = LocalDateTime.now();

            List<VehicleCache> cacheEntries = vehicles.stream()
                    .map(dto -> vehicleCacheMapper.toEntity(dto, syncDate))
                    .collect(Collectors.toList());

            vehicleCacheRepository.saveAll(cacheEntries);

            cleanOldCache();

            log.info("Vehicle cache updated successfully");
        } catch (Exception e) {
            log.error("Error updating vehicle cache", e);
            throw new RuntimeException("Failed to update vehicle cache", e);
        }
    }

    public Page<VehicleDTO> getFromCache(LocalDate dataInicio, LocalDate dataFim,
                                         String credor, String contrato,
                                         String protocolo, String cpf,
                                         String uf, String cidade,
                                         String modelo, String placa,
                                         String etapaAtual, String statusApreensao,
                                         Pageable pageable) {

        log.info("Retrieving vehicles from cache");

        Page<VehicleCache> cachedVehicles = vehicleCacheRepository.findWithFilters(
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