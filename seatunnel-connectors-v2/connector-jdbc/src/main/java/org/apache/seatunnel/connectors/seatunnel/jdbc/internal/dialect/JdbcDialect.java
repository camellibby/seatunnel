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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.stringtemplate.v4.ST;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */
public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object to seatunnel internal object.
     *
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter();

    /**
     * get jdbc meta-information type to seatunnel data type mapper.
     *
     * @return a type mapper for the database
     */
    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

    /**
     * Quotes the identifier for table name or field name
     */
    default String quoteIdentifier(String identifier) {
        return identifier;
    }

    default String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    /**
     * Constructs the dialects insert statement for a single row. The returned string will be used
     * as a {@link java.sql.PreparedStatement}. Fields in the statement must be in the same order as
     * the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * INSERT INTO table_name (column_name [, ...]) VALUES (value [, ...])
     * }</pre>
     *
     * @return the dialects {@code INSERT INTO} statement.
     */
    default String getInsertIntoStatement(String database, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName)
                        .collect(Collectors.joining(", "));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableIdentifier(database, tableName), columns, placeholders);
    }

    /**
     * Constructs the dialects update statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * UPDATE table_name SET col = val [, ...] WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code UPDATE} statement.
     */
    default String getUpdateStatement(
            String database, String tableName, String[] fieldNames, String[] conditionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "UPDATE %s SET %s WHERE %s",
                tableIdentifier(database, tableName), setClause, conditionClause);
    }

    /**
     * Constructs the dialects delete statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * DELETE FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code DELETE} statement.
     */
    default String getDeleteStatement(String database, String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "DELETE FROM %s WHERE %s", tableIdentifier(database, tableName), conditionClause);
    }

    /**
     * Generates a query to determine if a row exists in the table. The returned string will be used
     * as a {@link java.sql.PreparedStatement}.
     *
     * <pre>{@code
     * SELECT 1 FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code QUERY} statement.
     */
    default String getRowExistsStatement(
            String database, String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(field -> format("%s = :%s", quoteIdentifier(field), field))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "SELECT 1 FROM %s WHERE %s",
                tableIdentifier(database, tableName), fieldExpressions);
    }

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}.
     *
     * <p>If supported, the returned string will be used as a {@link java.sql.PreparedStatement}.
     * Fields in the statement must be in the same order as the {@code fieldNames} parameter.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT ROW Exists} + {@code UPDATE}/{@code INSERT} which may have poor performance.
     *
     * @return the dialects {@code UPSERT} statement or {@link Optional#empty()}.
     */
    Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields);

    /**
     * Different dialects optimize their PreparedStatement
     *
     * @return The logic about optimize PreparedStatement
     */
    default PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    default ResultSetMetaData getResultSetMetaData(
            Connection conn, JdbcSourceConfig jdbcSourceConfig) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(jdbcSourceConfig.getQuery());
        return ps.getMetaData();
    }

    /**
     *  默认为oracle写法 每个数据库需要单独实现
     *  oracle 是默认 nulls last 其他数据库要注意把 null 放在最后面
     *  为了防止 有人修改了数据库排序规则 强制把空值的放在最后面去
     * @param conn
     * @param jdbcSourceConfig
     * @param parameterValues
     * @return
     */
    default ResultSet getSplitValue(Connection conn, JdbcSourceConfig jdbcSourceConfig, Object[] parameterValues) {
        String template = "select * " +
                "  from (select <partitionColumn>, " +
                "               Row_number() over(order by <partitionColumn>) hang " +
                "          from (select distinct  <partitionColumn>  <partitionColumn> " +
                "                  from (<query>)) )" +
                " where hang in (<parameterValues>) " +
                " order by <partitionColumn> asc nulls last";
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

    /**
     * 默认为oracle写法 每个数据库需要单独实现
     * 下面的sql把 空值列 也算成了单独的一项 其他数据库也要照此写法
     * @param columnName
     * @param config
     * @return
     */
    default String getPartitionColumnCount(String columnName, JdbcSourceConfig config) {
        String template = "select sum(sl) " +
                "  from (SELECT COUNT(DISTINCT <columnName>) sl " +
                "          FROM (<tableName>) " +
                "        union all " +
                "        select 1 " +
                "          from (<tableName>) " +
                "         where <columnName> is null " +
                "           and rownum = 1) a";
        ST st = new ST(template);
        st.add("columnName", columnName);
        st.add("tableName", config.getQuery());
        return st.render();
    }

    default String getPartitionSql(String partitionColumn, String nativeSql) {
        return String.format(
                "SELECT * FROM (%s) tt where %s >= ? AND %s <= ?",
                nativeSql, partitionColumn, partitionColumn);
    }

}
