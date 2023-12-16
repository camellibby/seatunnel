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

package com.qh.dialect.oracle;


import com.qh.config.JdbcSinkConfig;
import com.qh.converter.ColumnMapper;
import com.qh.converter.JdbcRowConverter;
import com.qh.dialect.JdbcConnectorException;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.JdbcDialectTypeMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.stringtemplate.v4.ST;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.qh.dialect.oracle.OracleTypeMapper.*;
import static java.lang.String.format;

public class OracleDialect implements JdbcDialect {

    private static final int DEFAULT_ORACLE_FETCH_SIZE = 128;

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new OracleJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new OracleTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(tableName);
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

        String usingClause = String.format("SELECT %s FROM DUAL", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String upsertSQL =
                String.format(
                        " MERGE INTO %s TARGET"
                                + " USING (%s) SOURCE"
                                + " ON (%s) "
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s)",
                        tableIdentifier(database, tableName),
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_ORACLE_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public void setPreparedStatementValueByDbType(int position, PreparedStatement preparedStatement, String oracleType, String value) throws SQLException {
        switch (oracleType) {
            case ORACLE_INTEGER:
                preparedStatement.setInt(position, Integer.parseInt(value));
                break;
            case ORACLE_FLOAT:
                preparedStatement.setFloat(position, Float.parseFloat(value));
                break;
            case ORACLE_NUMBER:
                preparedStatement.setBigDecimal(position, new BigDecimal(value));
                break;
            case ORACLE_BINARY_DOUBLE:
                preparedStatement.setDouble(position, Double.parseDouble(value));
                break;
            case ORACLE_BINARY_FLOAT:
            case ORACLE_REAL:
                preparedStatement.setFloat(position, Float.parseFloat(value));
                break;
            case ORACLE_CHAR:
            case ORACLE_NCHAR:
            case ORACLE_NVARCHAR2:
            case ORACLE_VARCHAR2:
            case ORACLE_LONG:
            case ORACLE_ROWID:
            case ORACLE_NCLOB:
            case ORACLE_CLOB:
                preparedStatement.setString(position, value);
                break;
            case ORACLE_DATE:
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    java.util.Date date = df.parse(value);
                    preparedStatement.setDate(position, new java.sql.Date(date.getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            case ORACLE_TIMESTAMP:
            case ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    java.util.Date date = df1.parse(value);
                    Timestamp timestamp = new Timestamp(date.getTime());
                    preparedStatement.setTimestamp(position, timestamp);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            case ORACLE_BLOB:
            case ORACLE_RAW:
            case ORACLE_LONG_RAW:
            case ORACLE_BFILE:
                preparedStatement.setBytes(position, value.getBytes());
                break;
            // Doesn't support yet
            case ORACLE_UNKNOWN:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support ORACLE type '%s' on column '%s'  yet.",
                                oracleType, oracleType));

        }
    }

    public ResultSetMetaData getResultSetMetaData(
            Connection conn, JdbcSinkConfig jdbcSourceConfig) throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach((k, v) -> {
            columns.add("\"" + v + "\"");
        });
        String sql = String.format("select  %s from %s.%s where 1=2 ", StringUtils.join(columns, ","), jdbcSourceConfig.getDbSchema(), table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }


    public void updateData(Connection connection,
                           JdbcSinkConfig jdbcSinkConfig,
                           List<ColumnMapper> columnMappers,
                           List<ColumnMapper> listUc,
                           HashMap<List<String>, SeaTunnelRow> rows,
                           Map<String, String> metaDataHash

    ) throws SQLException {
        String templateInsert = "update <dbSchema>.<table> set " +
                "<columns:{sub | \"<sub.sinkColumnName>\" = ? }; separator=\", \"> " +
                " where  <pks:{pk | \"<pk.sinkColumnName>\" = ? }; separator=\" and \"> ";
        ST template = new ST(templateInsert);
        template.add("dbSchema", jdbcSinkConfig.getDbSchema());
        template.add("table", jdbcSinkConfig.getTable());
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

    public String getSinkQueryUpdate(List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString = " select <columns:{sub | \"<sub.sinkColumnName>\" }; separator=\", \"> " +
                "  from <dbSchema>.<table> a " +
                " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("dbSchema", jdbcSinkConfig.getDbSchema());
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            String tmpWhere = "( <ucs:{uc | \"<uc.sinkColumnName>\" = ?  }; separator=\" and \">  )";
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

    public String insertTableSql(JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        List<String> newColumns = columns.stream().map(x -> "\"" + x + "\"").collect(Collectors.toList());
        String sql = "insert into "
                + jdbcSinkConfig.getDbSchema()
                + "."
                + jdbcSinkConfig.getTable()
                + String.format("(%s)", StringUtils.join(newColumns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }


    public String copyTableOnlyColumn(String sourceTable, String targetTable, JdbcSinkConfig jdbcSinkConfig) {
        List<String> collect = jdbcSinkConfig.getPrimaryKeys().stream().map(x -> "\"" + x + "\"").collect(Collectors.toList());
        return format("create  table %s.%s as select  %s from %s.%s where 1=2 ",
                jdbcSinkConfig.getDbSchema(),
                targetTable,
                StringUtils.join(collect, ','),
                jdbcSinkConfig.getDbSchema(),
                sourceTable
        );
    }

    public String truncateTable(JdbcSinkConfig jdbcSinkConfig) {
        return String.format("truncate  table %s.%s", jdbcSinkConfig.getDbSchema(), jdbcSinkConfig.getTable());
    }

    public String dropTable(JdbcSinkConfig jdbcSinkConfig, String tableName) {
        return String.format("drop table  %s.%s", jdbcSinkConfig.getDbSchema(), tableName);
    }

    public String createIndex(String tmpTableName, JdbcSinkConfig jdbcSinkConfig) {
        List<String> collect = jdbcSinkConfig.getPrimaryKeys().stream().map(x -> "\"" + x + "\"").collect(Collectors.toList());
        return String.format(
                "CREATE UNIQUE INDEX %s.%s ON %s.%s(%s)",
                jdbcSinkConfig.getDbSchema(),
                tmpTableName,
                jdbcSinkConfig.getDbSchema(),
                tmpTableName,
                StringUtils.join(collect, ',')
        );
    }

    public String getSinkQueryZipper(List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString = "select  " +
                " <columns:{sub | \"<sub.sinkColumnName>\"  }; separator=\", \"> " +
                "  from (select   " +
                " <columns:{sub | \"<sub.sinkColumnName>\"  }; separator=\", \"> " +
                "               ,OPERATEFLAG,row_number() over(partition by <ucs:{uc | \"<uc.sinkColumnName>\"   }; separator=\", \"> order by OPERATETIME desc) hang  " +
                "          from <dbSchema>.<table>  " +
                "         )  " +
                " where hang = 1  " +
                "   and  OPERATEFLAG in ('I', 'U')" +
                "   and <filter> ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("dbSchema", jdbcSinkConfig.getDbSchema());
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        sqlQueryTemplate.add("ucs", ucColumns);
        if (rowSize == 0) {
            sqlQueryTemplate.add("filter", "1=2");
        } else {
            String where = "(";
            List<String> collect = ucColumns.stream().map(x -> "\"" + x.getSinkColumnName() + "\"").collect(Collectors.toList());
            String join = StringUtils.join(collect, ',');
            where += join + ") in ( %s ) ";
            List<String> tmp1 = new ArrayList<>();
            for (int i1 = 0; i1 < ucColumns.size(); i1++) {
                tmp1.add("?");
            }
            String tmp2 = String.format("(%s)", StringUtils.join(tmp1, ","));

            List<String> tmp3 = new ArrayList<>();
            for (int i = 0; i < rowSize; i++) {
                tmp3.add(tmp2);
            }
            where = String.format(where, StringUtils.join(tmp3, ","));
            sqlQueryTemplate.add("filter", where);
        }
        String sqlQuery = sqlQueryTemplate.render();
        return sqlQuery;
    }

    public int deleteDataZipper(Connection connection, JdbcSinkConfig jdbcSinkConfig, List<ColumnMapper> columnMappers, LocalDateTime startTime) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        int insert = 0;
        String insertSql1 = "select count(1) sl " +
                "  from (select * " +
                "          from (select <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \">, " +
                "                       OPERATEFLAG, " +
                "                       row_number() over(partition by <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \"> order by OPERATETIME desc) hang " +
                "                  from <dbSchema>.<table>) " +
                "         where hang = 1 " +
                "           and OPERATEFLAG in ('I', 'U')) " +
                " where <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \"> not in (select <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \"> from <dbSchema>.<ucTable>)";
        ST template1 = new ST(insertSql1);
        template1.add("dbSchema", jdbcSinkConfig.getDbSchema());
        template1.add("table", jdbcSinkConfig.getTable());
        template1.add("pks", ucColumns);
        template1.add("ucTable", "UC_" + jdbcSinkConfig.getTable());
        template1.add("operateTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String render1 = template1.render();
        PreparedStatement preparedStatement1 = null;
        try {
            preparedStatement1 = connection.prepareStatement(render1);
            ResultSet resultSet = preparedStatement1.executeQuery();
            if (resultSet.next()) {
                insert = resultSet.getInt("sl");
            }
            preparedStatement1.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String insertSql = "insert into <table>" +
                "  (<columns:{sub | \"<sub.sinkColumnName>\"  }; separator=\", \">, operateFlag, operateTime)" +
                " select  " +
                "  <columns:{sub | \"<sub.sinkColumnName>\" }; separator=\", \">, 'D' operateFlag, '<operateTime>' operateTime" +
                "  from (select *  " +
                "          from (select <columns:{sub | \"<sub.sinkColumnName>\" }; separator=\", \">,operateFlag,  " +
                "                       row_number() over(partition by <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \"> order by OPERATETIME desc) hang  " +
                "                  from <dbSchema>.<table>)  " +
                "         where hang = 1  " +
                "           and OPERATEFLAG in ('I', 'U'))  " +
                " where <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \"> not in (select <pks:{pk | \"<pk.sinkColumnName>\" }; separator=\", \"> from  <dbSchema>.<ucTable> )";
        ST template = new ST(insertSql);
        template.add("dbSchema", jdbcSinkConfig.getDbSchema());
        template.add("table", jdbcSinkConfig.getTable());
        template.add("columns", columnMappers);
        template.add("pks", ucColumns);
        template.add("ucTable", "UC_" + jdbcSinkConfig.getTable());
        template.add("operateTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String render = template.render();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(render);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return insert;
    }
}
