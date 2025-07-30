package com.montreal.msiav_bh.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "vehicle_cache")
public class VehicleCache {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "external_id")
    private Long externalId;

    @Column(name = "credor")
    private String credor;

    @Column(name = "data_pedido")
    private LocalDate dataPedido;

    @Column(name = "contrato", unique = true)
    private String contrato;

    @Column(name = "placa")
    private String placa;

    @Column(name = "modelo")
    private String modelo;

    @Column(name = "uf")
    private String uf;

    @Column(name = "cidade")
    private String cidade;

    @Column(name = "cpf_devedor")
    private String cpfDevedor;

    @Column(name = "protocolo", unique = true)
    private String protocolo;

    @Column(name = "etapa_atual")
    private String etapaAtual;

    @Column(name = "status_apreensao")
    private String statusApreensao;

    @Column(name = "ultima_movimentacao")
    private LocalDate ultimaMovimentacao;

    @Column(name = "created_at", nullable = false, updatable = false)
    @CreationTimestamp
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    @UpdateTimestamp
    private LocalDateTime updatedAt;

    @Column(name = "api_sync_date")
    private LocalDateTime apiSyncDate;
}