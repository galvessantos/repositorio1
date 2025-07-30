package com.montreal.msiav_bh.repository;

import com.montreal.msiav_bh.entity.VehicleCache;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface VehicleCacheRepository extends JpaRepository<VehicleCache, Long>, JpaSpecificationExecutor<VehicleCache> {

    Optional<VehicleCache> findByProtocolo(String protocolo);

    Optional<VehicleCache> findByContrato(String contrato);

    @Query("SELECT v FROM VehicleCache v WHERE v.apiSyncDate = (SELECT MAX(vc.apiSyncDate) FROM VehicleCache vc)")
    Page<VehicleCache> findLatestCachedVehicles(Pageable pageable);

    @Query("SELECT MAX(v.apiSyncDate) FROM VehicleCache v")
    Optional<LocalDateTime> findLastSyncDate();

    @Modifying
    @Query("DELETE FROM VehicleCache v WHERE v.apiSyncDate < :cutoffDate")
    void deleteOldCacheEntries(@Param("cutoffDate") LocalDateTime cutoffDate);

    @Query("SELECT v FROM VehicleCache v WHERE " +
            "(:dataInicio IS NULL OR v.dataPedido >= :dataInicio) AND " +
            "(:dataFim IS NULL OR v.dataPedido <= :dataFim) AND " +
            "(:credor IS NULL OR LOWER(v.credor) LIKE LOWER(CONCAT('%', :credor, '%'))) AND " +
            "(:contrato IS NULL OR v.contrato = :contrato) AND " +
            "(:protocolo IS NULL OR v.protocolo = :protocolo) AND " +
            "(:cpf IS NULL OR v.cpfDevedor = :cpf) AND " +
            "(:uf IS NULL OR v.uf = :uf) AND " +
            "(:cidade IS NULL OR LOWER(v.cidade) LIKE LOWER(CONCAT('%', :cidade, '%'))) AND " +
            "(:modelo IS NULL OR LOWER(v.modelo) LIKE LOWER(CONCAT('%', :modelo, '%'))) AND " +
            "(:placa IS NULL OR v.placa = :placa) AND " +
            "(:etapaAtual IS NULL OR v.etapaAtual = :etapaAtual) AND " +
            "(:statusApreensao IS NULL OR v.statusApreensao = :statusApreensao)")
    Page<VehicleCache> findWithFilters(@Param("dataInicio") LocalDate dataInicio,
                                       @Param("dataFim") LocalDate dataFim,
                                       @Param("credor") String credor,
                                       @Param("contrato") String contrato,
                                       @Param("protocolo") String protocolo,
                                       @Param("cpf") String cpf,
                                       @Param("uf") String uf,
                                       @Param("cidade") String cidade,
                                       @Param("modelo") String modelo,
                                       @Param("placa") String placa,
                                       @Param("etapaAtual") String etapaAtual,
                                       @Param("statusApreensao") String statusApreensao,
                                       Pageable pageable);
}