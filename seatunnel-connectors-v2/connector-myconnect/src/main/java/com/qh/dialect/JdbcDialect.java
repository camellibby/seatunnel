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

package com.qh.dialect;


import com.qh.config.JdbcSinkConfig;
import com.qh.converter.ColumnMapper;
import com.qh.converter.JdbcRowConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.stringtemplate.v4.ST;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
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
     * as a {@link PreparedStatement}. Fields in the statement must be in the same order as
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
     * returned string will be used as a {@link PreparedStatement}. Fields in the statement
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
     * returned string will be used as a {@link PreparedStatement}. Fields in the statement
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
     * as a {@link PreparedStatement}.
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
     * <p>If supported, the returned string will be used as a {@link PreparedStatement}.
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
            Connection conn, JdbcSinkConfig jdbcSourceConfig) throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach((k, v) -> {
            columns.add(v);
        });
        String sql = String.format("select  %s from %s where 1=2 ", StringUtils.join(columns, ","), table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }

    default void setPreparedStatementValueByDbType(int position, PreparedStatement preparedStatement, String dbType, String value) throws SQLException {
        preparedStatement.setString(position, value);
    }

    default void setPreparedStatementValue(PreparedStatement preparedStatement, int position, Object value) throws SQLException {
        if (null != value) {
            if (value instanceof Date) {
                preparedStatement.setTimestamp(position, new Timestamp(((Date) value).getTime()));
            } else if (value instanceof LocalDate) {
                preparedStatement.setDate(position, java.sql.Date.valueOf((LocalDate) value));
            } else if (value instanceof Integer) {
                preparedStatement.setInt(position, (Integer) value);
            } else if (value instanceof Long) {
                preparedStatement.setLong(position, (Long) value);
            } else if (value instanceof Double) {
                preparedStatement.setDouble(position, (Double) value);
            } else if (value instanceof Float) {
                preparedStatement.setFloat(position, (Float) value);
            } else if (value instanceof LocalDateTime) {
                preparedStatement.setTimestamp(position, Timestamp.valueOf((LocalDateTime) value));
            } else if (value instanceof BigDecimal) {
                preparedStatement.setBigDecimal(position, (BigDecimal) value);
            } else {
                preparedStatement.setString(position, (String) value);
            }
        } else {
            preparedStatement.setNull(position, Types.NULL);
        }

    }

    default String getSinkQueryUpdate(String tableName, List<ColumnMapper> columnMappers, int rowSize) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString = " select <columns:{sub | <sub.sinkColumnName>}; separator=\", \"> " +
                "  from <table> a " +
                " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", tableName);
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

    default String getSinkQueryZipper(String tableName, List<ColumnMapper> columnMappers, int rowSize) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString = " select <columns:{sub | <sub.sinkColumnName>}; separator=\", \"> " +
                "  from <table> a " +
                " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", tableName);
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            String tmpWhere = "( <ucs:{uc | <uc.sinkColumnName> = ?  }; separator=\" and \"> and ZIPPERFLAG='N' )";
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

    default String copyTableOnlyColumn(String sourceTable, String targetTable, List<String> columns) {
        return format("create  table %s as select  %s from %s where 1=2 ",
                targetTable,
                StringUtils.join(columns, ','),
                sourceTable
        );
    }

    default void updateData(Connection connection,
                            String table,
                            List<ColumnMapper> columnMappers,
                            List<ColumnMapper> listUc,
                            HashMap<List<String>, SeaTunnelRow> rows,
                            Map<String, String> metaDataHash

    ) throws SQLException {
        String templateInsert = "update <table> set " +
                "<columns:{sub | <sub.sinkColumnName> = ? }; separator=\", \"> " +
                " where  <pks:{pk | <pk.sinkColumnName> = ? }; separator=\" and \"> ";
        ST template = new ST(templateInsert);
        template.add("table", table);
        template.add("columns", columnMappers);
        template.add("pks", listUc);
        String updateSql = template.render();
        PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
        for (SeaTunnelRow row : rows.values()) {
            for (int i = 0; i < columnMappers.size(); i++) {
                String column = columnMappers.get(i).getSinkColumnName();
                String dbType = metaDataHash.get(column);
                this.setPreparedStatementValueByDbType(
                        i + 1,
                        preparedStatement,
                        dbType,
                        (String) row.getField(columnMappers.get(i).getSinkRowPosition()));
            }
            for (int i = 0; i < listUc.size(); i++) {
                String column = listUc.get(i).getSinkColumnName();
                String dbType = metaDataHash.get(column);
                this.setPreparedStatementValueByDbType(
                        i + 1 + columnMappers.size(),
                        preparedStatement,
                        dbType,
                        (String) row.getField(listUc.get(i).getSinkRowPosition()));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    default int deleteData(Connection connection, String table, String ucTable, List<ColumnMapper> ucColumns) {
        String delSql = "delete from  <table> a   " +
                " where not exists " +
                "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> b where <pks:{pk | a.<pk.sinkColumnName>=b.<pk.sinkColumnName> }; separator=\" and \">  ) ";
        ST template = new ST(delSql);
        template.add("table", table);
        template.add("tmpTable", ucTable);
        template.add("pks", ucColumns);
        PreparedStatement preparedStatement = null;
        int del = 0;
        try {
            preparedStatement = connection.prepareStatement(template.render());
            del = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return del;
    }

    default int deleteDataZipper(Connection connection, String table, String ucTable, List<ColumnMapper> columnMappers, LocalDateTime startTime) {
        return 0;
    }


    default Long getTableCount(Connection connection, String table) {
        long count = 0L;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(format("select  count(1) sl  from %s", table));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("sl");
                preparedStatement.close();
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
