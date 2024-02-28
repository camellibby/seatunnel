package com.qh.myconnect.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SeaTunnelJobsHistoryErrorRecord implements Serializable {
    private static final long serialVersionUID = -1L;
    private String flinkJobId;
    private String dataSourceId;
    private String dbSchema;
    private String tableName;
    private String errorData;
    private String errorMessage;
}
