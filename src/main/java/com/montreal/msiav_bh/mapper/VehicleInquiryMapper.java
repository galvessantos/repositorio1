package com.montreal.msiav_bh.mapper;

import com.montreal.msiav_bh.dto.VehicleDTO;
import com.montreal.msiav_bh.dto.response.ConsultaNotificationResponseDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class VehicleInquiryMapper {

    private static final Logger logger = LoggerFactory.getLogger(VehicleInquiryMapper.class);

    public List<VehicleDTO> mapToVeiculoDTO(List<ConsultaNotificationResponseDTO.NotificationData> notifications) {
        if (notifications == null || notifications.isEmpty()) {
            return Collections.emptyList();
        }

        List<VehicleDTO> veiculos = new ArrayList<>();

        for (var notification : notifications) {
            if (notification == null) continue;

            if (notification.veiculos() != null && !notification.veiculos().isEmpty()) {
                for (var veiculo : notification.veiculos()) {
                    var dto = createVeiculoDTOWithVehicleInfo(notification, veiculo);
                    if (dto != null) veiculos.add(dto);
                }
            } else {
                var dto = createVeiculoDTOFromNotification(notification);
                if (dto != null) veiculos.add(dto);
            }
        }

        return veiculos;
    }

    private VehicleDTO createVeiculoDTOFromNotification(ConsultaNotificationResponseDTO.NotificationData notification) {
        try {
            String credor = notification.nomeCredor() != null ? notification.nomeCredor() :
                    (notification.credor() != null && !notification.credor().isEmpty() ?
                            notification.credor().get(0).nome() : "N/A");

            String contrato = notification.numeroContrato() != null ? notification.numeroContrato() :
                    (notification.contrato() != null && !notification.contrato().isEmpty() ?
                            notification.contrato().get(0).numero() : "N/A");

            String protocolo = notification.protocolo() != null ? notification.protocolo() :
                    (notification.contrato() != null && !notification.contrato().isEmpty() ?
                            notification.contrato().get(0).protocolo() : "N/A");

            LocalDate dataPedido = parseDate(notification.dataPedido());
            LocalDate dataMovimentacao = parseDate(notification.dataMovimentacao());

            if (notification.contrato() != null && !notification.contrato().isEmpty()) {
                var contratoInfo = notification.contrato().get(0);
                if (dataPedido == null) dataPedido = contratoInfo.dataPedido();
                if (dataMovimentacao == null) dataMovimentacao = contratoInfo.dataNotificacao();
            }

            String cpfDevedor = notification.devedor() != null && !notification.devedor().isEmpty() ?
                    notification.devedor().get(0).cpfCnpj() : "N/A";

            String placa = "N/A";
            String modelo = "N/A";
            String uf = "N/A";

            if (notification.veiculos() != null && !notification.veiculos().isEmpty()) {
                var veiculo = notification.veiculos().get(0);
                placa = veiculo.placa() != null ? veiculo.placa() : "N/A";
                modelo = veiculo.modelo() != null ? veiculo.modelo() : "N/A";
                uf = veiculo.ufEmplacamento() != null ? veiculo.ufEmplacamento() : "N/A";
            }

            String cidade = extractCityFromNotification(notification);
            String etapaAtual = notification.etapa() != null ? notification.etapa() : determineEtapaAtual(notification);
            String statusApreensao = determineStatusApreensao(notification);

            return new VehicleDTO(null, credor, dataPedido, contrato, placa, modelo, uf, cidade, cpfDevedor, protocolo,
                    etapaAtual, statusApreensao, dataMovimentacao);

        } catch (Exception e) {
            return null;
        }
    }

    private VehicleDTO createVeiculoDTOWithVehicleInfo(ConsultaNotificationResponseDTO.NotificationData notification,
                                                       ConsultaNotificationResponseDTO.VeiculoInfo veiculo) {
        try {
            String credor = notification.nomeCredor() != null ? notification.nomeCredor() :
                    (notification.credor() != null && !notification.credor().isEmpty() ?
                            notification.credor().get(0).nome() : "N/A");

            String contrato = notification.numeroContrato() != null ? notification.numeroContrato() :
                    (notification.contrato() != null && !notification.contrato().isEmpty() ?
                            notification.contrato().get(0).numero() : "N/A");

            String protocolo = notification.protocolo() != null ? notification.protocolo() :
                    (notification.contrato() != null && !notification.contrato().isEmpty() ?
                            notification.contrato().get(0).protocolo() : "N/A");

            LocalDate dataPedido = parseDate(notification.dataPedido());
            LocalDate dataMovimentacao = parseDate(notification.dataMovimentacao());

            if (notification.contrato() != null && !notification.contrato().isEmpty()) {
                var contratoInfo = notification.contrato().get(0);
                if (dataPedido == null) dataPedido = contratoInfo.dataPedido();
                if (dataMovimentacao == null) dataMovimentacao = contratoInfo.dataNotificacao();
            }

            String cpfDevedor = notification.devedor() != null && !notification.devedor().isEmpty() ?
                    notification.devedor().get(0).cpfCnpj() : "N/A";

            String cidade = extractCityFromNotification(notification);
            String etapaAtual = notification.etapa() != null ? notification.etapa() : determineEtapaAtual(notification);
            String statusApreensao = determineStatusApreensaoFromVeiculo(veiculo);

            return new VehicleDTO(null, credor, dataPedido, contrato,
                    veiculo.placa() != null ? veiculo.placa() : "N/A",
                    veiculo.modelo() != null ? veiculo.modelo() : "N/A",
                    veiculo.ufEmplacamento() != null ? veiculo.ufEmplacamento() : "N/A",
                    cidade, cpfDevedor, protocolo, etapaAtual, statusApreensao, dataMovimentacao);

        } catch (Exception e) {
            return null;
        }
    }

    private String extractCityFromNotification(ConsultaNotificationResponseDTO.NotificationData notification) {
        if (notification.devedor() != null && !notification.devedor().isEmpty()) {
            var devedor = notification.devedor().get(0);
            if (devedor.enderecos() != null && !devedor.enderecos().isEmpty()) {
                for (var endereco : devedor.enderecos()) {
                    var cidade = extractCityFromAddress(endereco.endereco());
                    if (!"N/A".equals(cidade)) return cidade;
                }
            }
        }

        if (notification.contrato() != null && !notification.contrato().isEmpty()) {
            var contrato = notification.contrato().get(0);
            if (contrato.municipioContrato() != null && !contrato.municipioContrato().trim().isEmpty()) {
                return contrato.municipioContrato();
            }
        }

        return "N/A";
    }

    private LocalDate parseDate(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) return null;

        try {
            return LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")).toLocalDate();
        } catch (DateTimeParseException e1) {
            try {
                return LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toLocalDate();
            } catch (DateTimeParseException e2) {
                try {
                    return LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                } catch (DateTimeParseException e3) {
                    return null;
                }
            }
        }
    }

    private String extractCityFromAddress(String address) {
        if (address == null || address.trim().isEmpty()) return "N/A";

        String cleanAddress = address.replaceAll("\\d{5}-?\\d{3}", "").trim();
        String[] separators = {",", "-", "–", "/"};

        for (String separator : separators) {
            if (cleanAddress.contains(separator)) {
                String[] parts = cleanAddress.split(separator);
                for (int i = parts.length - 1; i >= 0; i--) {
                    String part = parts[i].trim();
                    if (part.length() > 2 && !part.matches("\\d+") && !part.matches("[A-Z]{2}")) {
                        part = part.replaceAll("\\d+", "").trim();
                        if (part.length() > 2) return part;
                    }
                }
            }
        }

        String[] words = cleanAddress.replaceAll("\\d+", " ").trim().split("\\s+");
        if (words.length >= 2) {
            String possibleCity = words[words.length - 2];
            if (possibleCity.length() > 2 && !possibleCity.matches("[A-Z]{2}")) {
                return possibleCity;
            }
        }

        return "N/A";
    }

    private String determineEtapaAtual(ConsultaNotificationResponseDTO.NotificationData notification) {
        if (notification.etapa() != null && !notification.etapa().trim().isEmpty()) return notification.etapa();

        if (notification.contrato() != null && !notification.contrato().isEmpty()) {
            var contrato = notification.contrato().get(0);
            if (contrato.dataCertidao() != null) return "Certidão de Busca Apreensão Emitida";
            if (contrato.dataPedido() != null) return "Pedido Registrado";
        }

        return "A iniciar";
    }

    private String determineStatusApreensao(ConsultaNotificationResponseDTO.NotificationData notification) {
        if (notification.veiculos() != null && !notification.veiculos().isEmpty()) {
            var veiculo = notification.veiculos().get(0);
            String status = determineStatusApreensaoFromVeiculo(veiculo);
            if (!"A iniciar".equals(status)) return status;
        }

        if (notification.contrato() != null && !notification.contrato().isEmpty()) {
            var contrato = notification.contrato().get(0);
            if (contrato.dataBaixaRestricao() != null) return "Concluído";
            if (contrato.nsu() != null && !contrato.nsu().trim().isEmpty()) return "Concluído";
            if (contrato.dataRestricao() != null || (contrato.numeroRestricao() != null && !contrato.numeroRestricao().trim().isEmpty()))
                return "Guincho acionado";
            if (contrato.certidaoBuscaApreensao() != null && !contrato.certidaoBuscaApreensao().trim().isEmpty())
                return "Localizador Acionado";
        }

        if (hasOrgaoAcionado(notification)) return "Localizador Acionado";

        return "A iniciar";
    }

    private String determineStatusApreensaoFromVeiculo(ConsultaNotificationResponseDTO.VeiculoInfo veiculo) {
        if ("S".equalsIgnoreCase(veiculo.possuiGps())) return "Localizador Acionado";
        if (veiculo.gravame() != null && !veiculo.gravame().trim().isEmpty()) return "Guincho acionado";
        if (veiculo.registroDetran() != null && !veiculo.registroDetran().trim().isEmpty()) return "Localizador Acionado";
        return "A iniciar";
    }

    private boolean hasOrgaoAcionado(ConsultaNotificationResponseDTO.NotificationData notification) {
        return notification.veiculos() != null && !notification.veiculos().isEmpty()
                && notification.contrato() != null && !notification.contrato().isEmpty()
                && notification.devedor() != null && !notification.devedor().isEmpty();
    }
}