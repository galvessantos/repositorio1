package com.montreal.msiav_bh.mapper;

import com.montreal.msiav_bh.dto.VehicleDTO;
import com.montreal.msiav_bh.entity.VehicleCache;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.time.LocalDateTime;

@Mapper(componentModel = "spring")
public interface VehicleCacheMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "externalId", source = "dto.id")
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    @Mapping(target = "apiSyncDate", source = "syncDate")
    VehicleCache toEntity(VehicleDTO dto, LocalDateTime syncDate);

    @Mapping(target = "id", source = "externalId")
    VehicleDTO toDTO(VehicleCache entity);
}