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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ClickHouseTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String CLICKHOUSE_UNKNOWN = "UNKNOWN";
    private static final String CLICKHOUSE_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String CLICKHOUSE_TINYINT = "TINYINT";

    private static final String CLICKHOUSE_UINT8 = "UINT8";
    private static final String CLICKHOUSE_UINT16 = "UINT16";
    private static final String CLICKHOUSE_UINT32 = "UINT32";
    private static final String CLICKHOUSE_UINT64 = "UINT64";







    private static final String CLICKHOUSE_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String CLICKHOUSE_SMALLINT = "SMALLINT";
    private static final String CLICKHOUSE_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String CLICKHOUSE_MEDIUMINT = "MEDIUMINT";
    private static final String CLICKHOUSE_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String CLICKHOUSE_INT = "INT";
    private static final String CLICKHOUSE_INT8 = "INT8";
    private static final String CLICKHOUSE_INT16 = "INT16";
    private static final String CLICKHOUSE_INT32 = "INT32";
    private static final String CLICKHOUSE_INT64 = "INT64";
    private static final String CLICKHOUSE_FLOAT64 = "FLOAT64";
    private static final String CLICKHOUSE_FLOAT32= "FLOAT32";

    private static final String CLICKHOUSE_INT_NULLABLE = "NULLABLE(INT)";
    private static final String CLICKHOUSE_INT8_NULLABLE = "NULLABLE(INT8)";
    private static final String CLICKHOUSE_INT16_NULLABLE = "NULLABLE(INT16)";
    private static final String CLICKHOUSE_INT32_NULLABLE = "NULLABLE(INT32)";
    private static final String CLICKHOUSE_INT64_NULLABLE = "NULLABLE(INT64)";
    private static final String CLICKHOUSE_FLOAT64_NULLABLE = "NULLABLE(FLOAT64)";
    private static final String CLICKHOUSE_FLOAT32_NULLABLE = "NULLABLE(FLOAT32)";
    private static final String CLICKHOUSE_NULLABLE_NOTHING = "NULLABLE(NOTHING)";
    private static final String CLICKHOUSE_NOTHING = "NOTHING";


    private static final String CLICKHOUSE_INT_UNSIGNED = "INT UNSIGNED";
    private static final String CLICKHOUSE_INTEGER = "INTEGER";
    private static final String CLICKHOUSE_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String CLICKHOUSE_BIGINT = "BIGINT";
    private static final String CLICKHOUSE_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String CLICKHOUSE_DECIMAL = "DECIMAL";
    private static final String CLICKHOUSE_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String CLICKHOUSE_FLOAT = "FLOAT";
    private static final String CLICKHOUSE_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String CLICKHOUSE_DOUBLE = "DOUBLE";
    private static final String CLICKHOUSE_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String CLICKHOUSE_STRING = "STRING";
    private static final String CLICKHOUSE_NULLABLE_STRING = "NULLABLE(STRING)";
    private static final String CLICKHOUSE_JSON = "JSON";

    private static final String CLICKHOUSE_LOWCARDINALITY_STRING = "LOWCARDINALITY(STRING)";

    // ------------------------------time-------------------------
    private static final String CLICKHOUSE_DATE = "DATE";
    private static final String CLICKHOUSE_NULLABLE_DATE = "NULLABLE(DATE)";
    private static final String CLICKHOUSE_DATETIME = "DATETIME";
    private static final String CLICKHOUSE_TIME = "TIME";
    private static final String CLICKHOUSE_NULLABLE_TIME = "NULLABLE(TIME)";
    private static final String CLICKHOUSE_TIMESTAMP = "TIMESTAMP";
    private static final String CLICKHOUSE_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String CLICKHOUSE_TINYBLOB = "TINYBLOB";
    private static final String CLICKHOUSE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String CLICKHOUSE_BLOB = "BLOB";
    private static final String CLICKHOUSE_LONGBLOB = "LONGBLOB";
    private static final String CLICKHOUSE_BINARY = "BINARY";
    private static final String CLICKHOUSE_VARBINARY = "VARBINARY";
    private static final String CLICKHOUSE_GEOMETRY = "GEOMETRY";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (mysqlType) {
            case CLICKHOUSE_BIT:
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case CLICKHOUSE_TINYINT:
            case CLICKHOUSE_TINYINT_UNSIGNED:
            case CLICKHOUSE_SMALLINT:
            case CLICKHOUSE_SMALLINT_UNSIGNED:
            case CLICKHOUSE_MEDIUMINT:
            case CLICKHOUSE_MEDIUMINT_UNSIGNED:
            case CLICKHOUSE_INT:
            case CLICKHOUSE_INT8:
            case CLICKHOUSE_INT16:
            case CLICKHOUSE_INT32:
            case CLICKHOUSE_INT64:

            case CLICKHOUSE_UINT8:
            case CLICKHOUSE_UINT16:
            case CLICKHOUSE_UINT32:
            case CLICKHOUSE_UINT64:


            case CLICKHOUSE_INT_NULLABLE:
            case CLICKHOUSE_INT8_NULLABLE:
            case CLICKHOUSE_INT16_NULLABLE:
            case CLICKHOUSE_INT32_NULLABLE:
            case CLICKHOUSE_INT64_NULLABLE:
            case CLICKHOUSE_INTEGER:
            case CLICKHOUSE_YEAR:
                return BasicType.INT_TYPE;
            case CLICKHOUSE_INT_UNSIGNED:
            case CLICKHOUSE_INTEGER_UNSIGNED:
            case CLICKHOUSE_BIGINT:
                return BasicType.LONG_TYPE;
            case CLICKHOUSE_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case CLICKHOUSE_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case CLICKHOUSE_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case CLICKHOUSE_FLOAT:
            case CLICKHOUSE_FLOAT32:
            case CLICKHOUSE_FLOAT64:
            case CLICKHOUSE_FLOAT32_NULLABLE:
            case CLICKHOUSE_FLOAT64_NULLABLE:
                return BasicType.FLOAT_TYPE;
            case CLICKHOUSE_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case CLICKHOUSE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case CLICKHOUSE_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case CLICKHOUSE_STRING:
            case CLICKHOUSE_NULLABLE_STRING:
            case CLICKHOUSE_LOWCARDINALITY_STRING:
            case CLICKHOUSE_JSON:
            case CLICKHOUSE_NULLABLE_NOTHING:
            case CLICKHOUSE_NOTHING :
                return BasicType.STRING_TYPE;
            case CLICKHOUSE_DATE:
            case CLICKHOUSE_NULLABLE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case CLICKHOUSE_TIME:
            case CLICKHOUSE_NULLABLE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case CLICKHOUSE_DATETIME:
            case CLICKHOUSE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case CLICKHOUSE_TINYBLOB:
            case CLICKHOUSE_MEDIUMBLOB:
            case CLICKHOUSE_BLOB:
            case CLICKHOUSE_LONGBLOB:
            case CLICKHOUSE_VARBINARY:
            case CLICKHOUSE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

            // Doesn't support yet
            case CLICKHOUSE_GEOMETRY:
            case CLICKHOUSE_UNKNOWN:
            default:
                if(mysqlType.toUpperCase().contains("DECIMAL")){
                    if (precision > 38) {
                        LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_DECIMAL);
                        return new DecimalType(38, 18);
                    }
                    return new DecimalType(precision, scale);
                }
                else if (mysqlType.toUpperCase().contains("DATETIME")) {
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                }
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new RuntimeException(
                        String.format(
                                "Doesn't support CLickHouse type '%s' on column '%s'  yet.请联系系统管理员",
                                mysqlType, jdbcColumnName));
        }
    }
}
