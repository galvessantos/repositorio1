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
import java.util.Set;

@Repository
public interface VehicleCacheRepository extends JpaRepository<VehicleCache, Long>, JpaSpecificationExecutor<VehicleCache> {

    Optional<VehicleCache> findByContrato(String contrato);

    Optional<VehicleCache> findByProtocolo(String protocolo);

    @Query("SELECT v FROM VehicleCache v WHERE v.apiSyncDate = (SELECT MAX(vc.apiSyncDate) FROM VehicleCache vc)")
    Page<VehicleCache> findLatestCachedVehicles(Pageable pageable);

    @Query("SELECT MAX(v.apiSyncDate) FROM VehicleCache v")
    Optional<LocalDateTime> findLastSyncDate();

    @Modifying
    @Query("DELETE FROM VehicleCache v WHERE v.apiSyncDate < :cutoffDate")
    void deleteOldCacheEntries(@Param("cutoffDate") LocalDateTime cutoffDate);

    @Modifying
    @Query("DELETE FROM VehicleCache v WHERE v.placa NOT IN :activePlacas")
    void deleteByPlacaNotIn(@Param("activePlacas") Set<String> activePlacas);

    @Query("SELECT COUNT(v) FROM VehicleCache v WHERE v.placa NOT IN :activePlacas")
    int countByPlacaNotIn(@Param("activePlacas") Set<String> activePlacas);

    Optional<VehicleCache> findByPlaca(String placa);

    @Query(value = "SELECT * FROM vehicle_cache v " +
            "WHERE (CAST(:dataInicio AS DATE) IS NULL OR v.data_pedido >= CAST(:dataInicio AS DATE)) " +
            "AND (CAST(:dataFim AS DATE) IS NULL OR v.data_pedido <= CAST(:dataFim AS DATE)) " +
            "AND (CAST(:credor AS VARCHAR) IS NULL OR UPPER(v.credor) LIKE UPPER(CONCAT('%', CAST(:credor AS VARCHAR), '%'))) " +
            "AND (CAST(:contrato AS VARCHAR) IS NULL OR v.contrato = CAST(:contrato AS VARCHAR)) " +
            "AND (CAST(:protocolo AS VARCHAR) IS NULL OR v.protocolo = CAST(:protocolo AS VARCHAR)) " +
            "AND (CAST(:cpf AS VARCHAR) IS NULL OR v.cpf_devedor = CAST(:cpf AS VARCHAR)) " +
            "AND (CAST(:uf AS VARCHAR) IS NULL OR v.uf = CAST(:uf AS VARCHAR)) " +
            "AND (CAST(:cidade AS VARCHAR) IS NULL OR UPPER(v.cidade) LIKE UPPER(CONCAT('%', CAST(:cidade AS VARCHAR), '%'))) " +
            "AND (CAST(:modelo AS VARCHAR) IS NULL OR UPPER(v.modelo) LIKE UPPER(CONCAT('%', CAST(:modelo AS VARCHAR), '%'))) " +
            "AND (CAST(:placa AS VARCHAR) IS NULL OR v.placa = CAST(:placa AS VARCHAR)) " +
            "AND (CAST(:etapaAtual AS VARCHAR) IS NULL OR v.etapa_atual = CAST(:etapaAtual AS VARCHAR)) " +
            "AND (CAST(:statusApreensao AS VARCHAR) IS NULL OR v.status_apreensao = CAST(:statusApreensao AS VARCHAR))",
            nativeQuery = true)
    Page<VehicleCache> findWithFiltersFixed(@Param("dataInicio") LocalDate dataInicio,
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