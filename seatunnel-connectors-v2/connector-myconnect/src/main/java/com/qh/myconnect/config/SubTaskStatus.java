package com.qh.myconnect.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SubTaskStatus implements Serializable {
    private static final long serialVersionUID = -1L;
    private String flinkJobId;
    private String dataSourceId;
    private String dbSchema = "null";
    private String tableName;
    private String subtaskIndexId;
    private String status;
}
