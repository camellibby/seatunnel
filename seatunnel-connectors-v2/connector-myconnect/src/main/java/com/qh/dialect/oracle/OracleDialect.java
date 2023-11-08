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



import com.qh.converter.JdbcRowConverter;
import com.qh.dialect.JdbcConnectorException;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.common.exception.CommonErrorCode;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qh.dialect.oracle.OracleTypeMapper.*;

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
}
