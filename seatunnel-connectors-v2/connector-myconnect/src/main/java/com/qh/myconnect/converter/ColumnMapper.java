package com.qh.myconnect.converter;

import lombok.Data;

import java.util.function.Function;

@Data
public class ColumnMapper {
    private String sourceColumnName;
    private String sourceColumnTypeName;
    private String sinkColumnName;
    private String sinkColumnTypeName;
    private Integer sourceRowPosition;
    private Integer sinkRowPosition;
    private boolean uc = false;
    private String sinkColumnDbType;
    private Function<Object, Object> converter = str ->  str;
}
