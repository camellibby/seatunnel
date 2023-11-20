package com.qh.dialect;

import com.qh.dialect.ClickHouse.ClickHouseDialect;
import com.qh.dialect.mysql.MysqlDialect;
import com.qh.dialect.oracle.OracleDialect;

import java.util.HashMap;

public class JdbcDialectFactory {

    private static final HashMap<String, JdbcDialect> map = new HashMap<String, JdbcDialect>() {
        {
            put("oracle", new OracleDialect());
            put("mysql", new MysqlDialect());
            put("clickhouse", new ClickHouseDialect());
        }
    };

    public static JdbcDialect getJdbcDialect(String param) {
        String lowerCase = param.toLowerCase();
        return map.get(lowerCase);

    }

}
