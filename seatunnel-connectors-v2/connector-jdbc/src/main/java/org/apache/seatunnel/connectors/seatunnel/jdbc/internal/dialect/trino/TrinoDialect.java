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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.trino;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.stringtemplate.v4.ST;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TrinoDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Trino";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new TrinoJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new TrinoTypeMapper();
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

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(2000);
        return statement;
    }

    public ResultSet getSplitValue(Connection conn, JdbcSourceConfig jdbcSourceConfig, Object[] parameterValues) {
        String template = "select *  " +
                "  from (SELECT <partitionColumn>, @i := @i + 1 hang  " +
                "          FROM (SELECT DISTINCT <partitionColumn>  " +
                "                  FROM (SELECT <partitionColumn>  " +
                "                          FROM (<query>) a) a  " +
                "                 ORDER BY <partitionColumn> is null  ASC, <partitionColumn> asc ) a,  " +
                "               (SELECT @i := 0) b) a  " +
                " where hang in (<parameterValues>)";
        ST st = new ST(template);
        st.add("partitionColumn", jdbcSourceConfig.getPartitionColumn().orElseThrow(() -> new NullPointerException("分区列为空")));
        st.add("query", jdbcSourceConfig.getQuery());
        List<Long> parameterValuesInt = Arrays.stream(parameterValues).map(x -> (Long) x).collect(Collectors.toList());
        st.add("parameterValues", StringUtils.join(parameterValuesInt, ","));
        String render = st.render();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(render);
            return preparedStatement.executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public String getPartitionColumnCount(String columnName, JdbcSourceConfig config) {
        String template = "SELECT sum(sl) sl " +
                "  FROM (SELECT COUNT(DISTINCT <columnName>) sl " +
                "          FROM (<tableName>) a " +
                "        UNION ALL " +
                "        SELECT * " +
                "          FROM (SELECT 1 FROM (<tableName>) a WHERE <columnName> IS NULL LIMIT 1) a) a";
        ST st = new ST(template);
        st.add("columnName", columnName);
        st.add("tableName", config.getQuery());
        return st.render();
    }

    public String getPartitionSql(String partitionColumn, String nativeSql) {
        return String.format(
                "SELECT * FROM (%s) tt where %s >= ? AND %s <= ?",
                nativeSql, partitionColumn, partitionColumn);
    }


}
