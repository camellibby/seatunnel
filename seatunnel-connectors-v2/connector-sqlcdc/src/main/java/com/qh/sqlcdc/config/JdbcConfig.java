package com.qh.sqlcdc.config;

import lombok.Data;

@Data
public class JdbcConfig {
    private String driver;
    private String query;
    private String url;
    private String user;
    private String passWord;
    private String dbType;
}
