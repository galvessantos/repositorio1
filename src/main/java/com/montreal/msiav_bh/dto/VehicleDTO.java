package com.montreal.msiav_bh.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDate;

public record VehicleDTO(
        Long id,
        String credor,

        @JsonFormat(pattern = "dd/MM/yyyy")
        LocalDate dataPedido,

        String contrato,
        String placa,
        String modelo,
        String uf,
        String cidade,
        String cpfDevedor,
        String protocolo,
        String etapaAtual,
        String statusApreensao,

        @JsonFormat(pattern = "dd/MM/yyyy")
        LocalDate ultimaMovimentacao
) {}
