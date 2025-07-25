package com.montreal.msiav_bh.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NotificationsDTO {

    @JsonProperty("electronicNotification")
    private ElectronicNotificationDTO electronicNotification;

    @JsonProperty("arNotification")
    private FileReportDTO  fileNotification;

}