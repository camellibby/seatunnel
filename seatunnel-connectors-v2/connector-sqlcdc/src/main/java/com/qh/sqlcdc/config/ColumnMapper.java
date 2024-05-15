package com.qh.sqlcdc.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class ColumnMapper implements Serializable {
    private static final long serialVersionUID = -1L;
    private String sourceColumnName;
    private String sourceColumnTypeName;
    private String sinkColumnName;
    private String sinkColumnTypeName;
    private Integer sourceRowPosition;
    private Integer sinkRowPosition;
    private boolean uc = false;
    private boolean primaryKey = false;
    private String sinkColumnDbType;
}
