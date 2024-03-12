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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

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

public class SqlServerDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Sqlserver";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SqlserverJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SqlserverTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "[SOURCE]." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "MERGE INTO %s.%s AS [TARGET]"
                                + " USING (%s) AS [SOURCE]"
                                + " ON (%s)"
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s);",
                        database,
                        tableName,
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    public  String getPartitionColumnCount(String columnName, JdbcSourceConfig config) {
        String template = "select sum(sl) " +
                "  from (SELECT COUNT(DISTINCT <columnName>) sl " +
                "          FROM (<tableName>) a" +
                "        union all " +
                "        select top 1 1 " +
                "          from (<tableName>) a " +
                "         where <columnName> is null " +
                "          ) a";
        ST st = new ST(template);
        st.add("columnName", columnName);
        st.add("tableName", config.getQuery());
        return st.render();
    }

    public ResultSet getSplitValue(Connection conn, JdbcSourceConfig jdbcSourceConfig, Object[] parameterValues) {
        String template = "select * " +
                "  from (select <partitionColumn>, " +
                "               Row_number() over(order by case when  <partitionColumn> is null then 1 else 0 end,  <partitionColumn> asc) hang " +
                "          from (select distinct  <partitionColumn>  <partitionColumn> " +
                "                  from (<query>) a ) a ) a" +
                " where hang in (<parameterValues>) " +
                " order by case when  <partitionColumn> is null then 1 else 0 end,  <partitionColumn> asc  ";
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
}
