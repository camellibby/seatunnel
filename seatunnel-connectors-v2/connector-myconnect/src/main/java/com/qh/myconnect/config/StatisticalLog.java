package com.qh.myconnect.config;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class StatisticalLog implements Serializable {
    private static final long serialVersionUID = -1L;
    private String dataSourceId;
    private String dbSchema;
    private String tableName;
    private Long writeCount;
    private Long modifyCount;
    private Long deleteCount;
    private Long keepCount;
    private Long insertCount;
    private Long errorCount;
    private String flinkJobId;
    private java.time.LocalDateTime startTime;
    private LocalDateTime endTime;
}
