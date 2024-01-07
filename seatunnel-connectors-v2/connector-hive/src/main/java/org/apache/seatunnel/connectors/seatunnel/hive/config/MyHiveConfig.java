package org.apache.seatunnel.connectors.seatunnel.hive.config;

import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

@Data
public class MyHiveConfig implements Serializable {
    private String user;
    private String password;
    private String jdbc_url;
    private String tableName;

    public MyHiveConfig(Config config) {
        this.tableName = config.getString(HiveConfig.TABLE_NAME.key());
        this.jdbc_url = config.getString(HiveConfig.JDBC_URL.key());
        this.user = config.getString(HiveConfig.USER.key());
        this.password = config.getString(HiveConfig.PASSWORD.key());
    }
}
