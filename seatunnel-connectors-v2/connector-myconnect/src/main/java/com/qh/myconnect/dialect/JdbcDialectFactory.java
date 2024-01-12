package com.qh.myconnect.dialect;

import com.qh.myconnect.dialect.ClickHouse.ClickHouseDialect;
import com.qh.myconnect.dialect.mysql.MysqlDialect;
import com.qh.myconnect.dialect.oracle.OracleDialect;
import com.qh.myconnect.dialect.pgsql.PostgresDialect;

import java.util.HashMap;

public class JdbcDialectFactory {

    private static final HashMap<String, JdbcDialect> map = new HashMap<String, JdbcDialect>() {
        {
            put("oracle", new OracleDialect());
            put("mysql", new MysqlDialect());
            put("clickhouse", new ClickHouseDialect());
            put("pgsql", new PostgresDialect());
        }
    };

    public static JdbcDialect getJdbcDialect(String param) {
        String lowerCase = param.toLowerCase();
        return map.get(lowerCase);

    }

}
