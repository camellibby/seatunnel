package com.qh.myconnect.config;

import lombok.Data;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public interface MySourceOptions {
   public Option<List<DbConfig>> DB_CONFIGS = Options.key("db_configs").listType(DbConfig.class).noDefaultValue().withDescription("db_configs");

    @Data
    static class DbConfig {
        private String url;
        private String driver;
        private String userName;
        private String passWord;
        private String query;
    }
}
