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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.hive.config.MyHiveConfig;

import java.io.IOException;
import java.sql.*;
import java.util.Optional;

@Slf4j
public class MyHiveReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final MyHiveConfig hiveConfig;
    private final SingleSplitReaderContext context;

    private Connection conn;

    private final SeaTunnelRowType typeInfo;

    MyHiveReader(MyHiveConfig sqlCdcConfig, SingleSplitReaderContext context, SeaTunnelRowType typeInfo) throws RuntimeException {
        String password = null;
        this.hiveConfig = sqlCdcConfig;
        if (null != this.hiveConfig.getPassword() && !hiveConfig.getPassword().equalsIgnoreCase("")) {
            password = hiveConfig.getPassword();
        }
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            this.conn = DriverManager.getConnection(hiveConfig.getJdbc_url(), hiveConfig.getUser(), password);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.context = context;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws IOException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            String sql = String.format("select * from %s ", this.hiveConfig.getTableName());
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();
            while (resultSet.next()) {
                SeaTunnelRow seaTunnelRow = toInternal(resultSet, typeInfo);
                output.collect(seaTunnelRow);
            }
            ps.close();
            context.signalNoMoreElement();
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
    }

    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = rs.getString(resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getBoolean(resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getByte(resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getShort(resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getInt(resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getLong(resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getFloat(resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getDouble(resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getBigDecimal(resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = rs.getDate(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new RuntimeException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE +
                                    "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }
}
