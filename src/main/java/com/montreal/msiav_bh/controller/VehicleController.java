package com.montreal.msiav_bh.controller;

import com.montreal.msiav_bh.dto.PageDTO;
import com.montreal.msiav_bh.dto.VehicleDTO;

import com.montreal.msiav_bh.service.ApiQueryService;
import com.montreal.msiav_bh.service.VehicleService;
import com.montreal.msiav_bh.utils.exceptions.ValidationMessages;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/vehicle")
@Tag(name = "Veiculos", description = "Veiculos")
@ApiResponses({
        @ApiResponse(responseCode = "401", description = "Acesso não autorizado"),
        @ApiResponse(responseCode = "404", description = "Recurso não encontrado")
})
public class VehicleController {
    private static final Logger logger = LoggerFactory.getLogger(VehicleController.class);

    @Autowired
    private VehicleService vehicleService;

    @Autowired
    private ApiQueryService apiQueryService;

    @GetMapping
    public ResponseEntity<?> buscarVeiculos(
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate dataInicio,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate dataFim,
            @RequestParam(required = false) String credor,
            @RequestParam(required = false) String contrato,
            @RequestParam(required = false) String protocolo,
            @RequestParam(required = false) @Pattern(regexp = "\\d{11,14}", message = ValidationMessages.CPF_INVALIDO) String cpf,
            @RequestParam(required = false) String uf,
            @RequestParam(required = false) String cidade,
            @RequestParam(required = false) String modelo,
            @RequestParam(required = false) String placa,
            @RequestParam(required = false) String etapaAtual,
            @RequestParam(required = false) String statusApreensao,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size,
            @RequestParam(defaultValue = "protocolo") String sortBy,
            @RequestParam(defaultValue = "asc") String sortDir) {

        // Define valores padrão se não forem passados na requisição
        if (dataInicio == null && dataFim == null) {
            dataInicio = LocalDate.of(2025, 3, 20);
            dataFim = LocalDate.of(2025, 7, 20);
        }

        if (dataInicio != null && dataFim != null && dataFim.isBefore(dataInicio)) {
            return ResponseEntity.badRequest().body(Map.of("error", ValidationMessages.DATA_FIM_ANTERIOR_INICIO));
        }

        if (dataInicio != null && dataFim == null) {
            return ResponseEntity.badRequest().body(Map.of("error", ValidationMessages.DATA_FIM_OBRIGATORIA));
        }

        if (dataFim != null && dataInicio == null) {
            return ResponseEntity.badRequest().body(Map.of("error", ValidationMessages.DATA_INICIO_OBRIGATORIA));
        }

        try {
            PageDTO<VehicleDTO> resultado = vehicleService.buscarComFiltros(
                    dataInicio, dataFim, credor, contrato, protocolo, cpf, uf, cidade,
                    modelo, placa, etapaAtual, statusApreensao, page, size, sortBy, sortDir
            );
            return ResponseEntity.ok(resultado);
        } catch (IllegalArgumentException e) {
            logger.error("Erro de validação: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            logger.error("Erro na busca: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", "Erro ao processar requisição"));
        }
    }


    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        try {
            vehicleService.healthCheck();
            return ResponseEntity.ok("API operacional");
        } catch (Exception e) {
            return ResponseEntity.status(503).body("Falha na conexão com a API externa");
        }
    }
}
