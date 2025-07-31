package com.montreal.msiav_bh.job;

import com.montreal.msiav_bh.context.CacheUpdateContext;
import com.montreal.msiav_bh.dto.VehicleDTO;
import com.montreal.msiav_bh.dto.response.ConsultaNotificationResponseDTO;
import com.montreal.msiav_bh.mapper.VehicleInquiryMapper;
import com.montreal.msiav_bh.service.ApiQueryService;
import com.montreal.msiav_bh.service.VehicleCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleCacheUpdateJob {

    private final ApiQueryService apiQueryService;
    private final VehicleInquiryMapper vehicleInquiryMapper;
    private final VehicleCacheService vehicleCacheService;

    @Value("${vehicle.cache.update.enabled:true}")
    private boolean cacheUpdateEnabled;

    @Value("${vehicle.cache.update.days-to-fetch:30}")
    private int daysToFetch;

    @Scheduled(fixedDelayString = "${vehicle.cache.update.interval:600000}")
    public void updateVehicleCache() {
        if (!cacheUpdateEnabled) {
            log.debug("Vehicle cache update is disabled");
            return;
        }

        log.info("Starting scheduled vehicle cache update");

        try {
            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusDays(daysToFetch);

            List<ConsultaNotificationResponseDTO.NotificationData> notifications =
                    apiQueryService.searchByPeriod(startDate, endDate);

            if (notifications != null && !notifications.isEmpty()) {
                List<VehicleDTO> vehicles = vehicleInquiryMapper.mapToVeiculoDTO(notifications);

                CacheUpdateContext context = CacheUpdateContext.scheduledRefresh(startDate, endDate);
                vehicleCacheService.updateCache(vehicles, context);

                log.info("Successfully updated cache with {} vehicles", vehicles.size());
            } else {
                log.warn("No vehicles found in API response for cache update");
            }

        } catch (Exception e) {
            log.error("Failed to update vehicle cache", e);
        }
    }

    @Scheduled(cron = "${vehicle.cache.cleanup.cron:0 0 2 * * ?}")
    public void cleanupOldCache() {
        log.info("Starting scheduled cache cleanup");

        try {
            vehicleCacheService.cleanOldCache();
            log.info("Cache cleanup completed successfully");
        } catch (Exception e) {
            log.error("Failed to cleanup old cache entries", e);
        }
    }
}