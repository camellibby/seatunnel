/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qh.myconnect.dialect.mysql;

import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.converter.JdbcRowConverter;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MysqlDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new MysqlJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new MySqlTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                        + "=VALUES("
                                        + quoteIdentifier(fieldName)
                                        + ")")
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                getInsertIntoStatement(database, tableName, fieldNames)
                + " ON DUPLICATE KEY UPDATE "
                + updateClause;
        return Optional.of(upsertSQL);
    }

    public ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSinkConfig jdbcSourceConfig)
            throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach(
                (k, v) -> {
                    columns.add("`" + v + "`");
                });
        String sql =
                String.format(
                        "select  %s from %s where 1=2 ", StringUtils.join(columns, ","), table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
        return statement;
    }

    public String insertTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        List<String> newColumns = columns.stream().map(x -> "`" + x + "`").collect(Collectors.toList());
        String sql =
                "insert into "
                + jdbcSinkConfig.getTable()
                + String.format("(%s)", StringUtils.join(newColumns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    public String insertTmpTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        List<String> newColumns = columns.stream().map(x -> "`" + x + "`").collect(Collectors.toList());
        String sql =
                "insert into "
                +"XJ$_" + jdbcSinkConfig.getTable()
                + String.format("(%s)", StringUtils.join(newColumns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    public String getSinkQueryUpdate(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString =
                " select <columns:{sub | `<sub.sinkColumnName>`}; separator=\", \"> "
                + "  from <table> a "
                + " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            String tmpWhere = "( <ucs:{uc | <uc.sinkColumnName> = ?  }; separator=\" and \">  )";
            ST tmpst = new ST(tmpWhere);
            tmpst.add("ucs", ucColumns);
            String render = tmpst.render();
            where.add(render);
        }
        String wheres = StringUtils.join(where, "  or ");
        if (rowSize == 0) {
            sqlQuery = sqlQuery + " 1=2";
        } else {
            sqlQuery = sqlQuery + wheres;
        }
        return sqlQuery;
    }
}
