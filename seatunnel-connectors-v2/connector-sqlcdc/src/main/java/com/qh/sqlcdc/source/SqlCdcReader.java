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

package com.qh.sqlcdc.source;

import com.alibaba.fastjson2.JSONObject;
import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SqlCdcReader implements SourceReader<SeaTunnelRow, SqlCdcSourceSplit> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SourceReader.Context context;

    private Connection conn;

    private final JdbcDialect jdbcDialect;
    private final SeaTunnelRowType typeInfo;

    private final Integer currentTaskId;
    private final Integer allTask;

    private Object startRowNumber;
    private Object endRowNumber;


    SqlCdcReader(SqlCdcConfig sqlCdcConfig, SourceReader.Context context, JdbcDialect jdbcDialect, SeaTunnelRowType typeInfo) {
        this.conn = new Util().getConnection(sqlCdcConfig);
        this.sqlCdcConfig = sqlCdcConfig;
        this.context = context;
        this.jdbcDialect = jdbcDialect;
        this.typeInfo = typeInfo;
        this.currentTaskId = context.getIndexOfSubtask();
        this.allTask = this.sqlCdcConfig.getPartitionNum();
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
        if(this.sqlCdcConfig.getDirectCompare()){
            directCompare(output);
        }else{
            normalCompare(output);
        }
    }

    private void checkDelete(Collector<SeaTunnelRow> output, Map.Entry<String, Date> entry, Map<String, Integer> mapPosition, Iterator<Map.Entry<String, Date>> iterator) throws SQLException {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(typeInfo.getTotalFields());
        seaTunnelRow.setRowKind(RowKind.DELETE);
        String checkRealDelete = this.jdbcDialect.checkRealDelete(this.sqlCdcConfig.getPrimaryKeys(), this.sqlCdcConfig.getQuery());
        PreparedStatement checkRealDeleteStatement = null;
        try {
            checkRealDeleteStatement = conn.prepareStatement(checkRealDelete);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] keys = entry.getKey().split(",");
        for (int i = 0; i < this.sqlCdcConfig.getPrimaryKeys().size(); i++) {
            String column = this.sqlCdcConfig.getPrimaryKeys().get(i);
            Integer position = mapPosition.get(column);
            SeaTunnelDataType<?> fieldType = typeInfo.getFieldType(position);
            SqlType sqlType = fieldType.getSqlType();
            switch (sqlType) {
                case STRING:
                    seaTunnelRow.setField(position, keys[i]);
                    checkRealDeleteStatement.setString(i + 1, keys[i]);
                    break;
                case BOOLEAN:
                    seaTunnelRow.setField(position, Boolean.parseBoolean(keys[i]));
                    checkRealDeleteStatement.setBoolean(i + 1, Boolean.parseBoolean(keys[i]));
                    break;
                case TINYINT:
                    seaTunnelRow.setField(position, Integer.parseInt(keys[i]));
                    checkRealDeleteStatement.setInt(i + 1, Integer.parseInt(keys[i]));
                    break;
                case SMALLINT:
                    seaTunnelRow.setField(position, Integer.parseInt(keys[i]));
                    checkRealDeleteStatement.setInt(i + 1, Integer.parseInt(keys[i]));
                    break;
                case INT:
                    seaTunnelRow.setField(position, Integer.parseInt(keys[i]));
                    checkRealDeleteStatement.setInt(i + 1, Integer.parseInt(keys[i]));
                    break;
                case BIGINT:
                    seaTunnelRow.setField(position, Integer.parseInt(keys[i]));
                    checkRealDeleteStatement.setInt(i + 1, Integer.parseInt(keys[i]));
                    break;
                case FLOAT:
                    seaTunnelRow.setField(position, Float.parseFloat(keys[i]));
                    checkRealDeleteStatement.setFloat(i + 1, Float.parseFloat(keys[i]));
                    break;
                case DOUBLE:
                    seaTunnelRow.setField(position, Double.parseDouble(keys[i]));
                    checkRealDeleteStatement.setDouble(i + 1, Double.parseDouble(keys[i]));
                    break;
                case DECIMAL:
                    seaTunnelRow.setField(position, new BigDecimal(keys[i]));
                    checkRealDeleteStatement.setBigDecimal(i + 1,new BigDecimal(keys[i]));
                    break;
                case DATE:
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = null;
                    try {
                        date = sdf.parse(keys[i]);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                    java.sql.Date sqlDate = new java.sql.Date(date.getTime());
                    seaTunnelRow.setField(position, sqlDate);
                    checkRealDeleteStatement.setDate(i + 1,sqlDate);
                    break;
                case TIME:
                    SimpleDateFormat sdf1 = new SimpleDateFormat("HH:mm:ss");
                    Date date1 = null;
                    try {
                        date1 = sdf1.parse(keys[i]);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                    Time sqlTime = new Time(date1.getTime());
                    seaTunnelRow.setField(position, sqlTime);
                    checkRealDeleteStatement.setTime(i + 1,sqlTime);
                    break;
                case TIMESTAMP:
                    String dateStr = keys[i];
                    SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Timestamp timestamp = null;
                    try {
                        timestamp = new Timestamp(sdf3.parse(dateStr).getTime());
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                    seaTunnelRow.setField(position, timestamp);
                    checkRealDeleteStatement.setTimestamp(i + 1,timestamp);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new RuntimeException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE +
                                    "Unexpected value: " + sqlType.toString());
            }
        }
        try {
            ResultSet resultSet2 = checkRealDeleteStatement.executeQuery();
            while (resultSet2.next()) {
                long aLong = resultSet2.getLong("sl");
                if (aLong == 0) {
                    output.collect(seaTunnelRow);
                    iterator.remove();
                }
            }
            checkRealDeleteStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SqlCdcSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<SqlCdcSourceSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private void directCompare(Collector<SeaTunnelRow> output){
        try {
            conn.setAutoCommit(false);
            if (sqlCdcConfig.getDbType().equalsIgnoreCase("mysql")) {
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
            Map<String, Integer> mapPosition = new HashMap<>();
            for (String primaryKey : this.sqlCdcConfig.getPrimaryKeys()) {
                int i = typeInfo.indexOf(primaryKey);
                mapPosition.put(primaryKey, i);
            }
            while (true) {
                String partitionColumnCountSql = this.jdbcDialect.getColumnDistinctCount(this.sqlCdcConfig.getPartitionColumn(), this.sqlCdcConfig);
                PreparedStatement psTotal = conn.prepareStatement(partitionColumnCountSql);
                ResultSet resultSet1 = psTotal.executeQuery();
                long totalSl = 0;
                int interval = 0;
                while (resultSet1.next()) {
                    totalSl = resultSet1.getLong("sl");
                    interval = (int) Math.ceil((double) totalSl / this.allTask);
                }
                psTotal.close();
                for (int i = 0; i < allTask; i++) {
                    if (i == currentTaskId) {
                        long startHang = i * interval + 1;
                        long endHang = (i + 1) * interval;
                        if (endHang > totalSl) {
                            endHang = totalSl;
                        }
                        PreparedStatement startPs = conn.prepareStatement(this.jdbcDialect.getHangValueSql(this.sqlCdcConfig, startHang));
                        ResultSet rsStart = startPs.executeQuery();
                        while (rsStart.next()) {
                            startRowNumber = rsStart.getObject(this.sqlCdcConfig.getPartitionColumn());
                        }
                        PreparedStatement endPs = conn.prepareStatement(this.jdbcDialect.getHangValueSql(this.sqlCdcConfig, endHang));
                        ResultSet rsEnd = endPs.executeQuery();
                        while (rsEnd.next()) {
                            endRowNumber = rsEnd.getObject(this.sqlCdcConfig.getPartitionColumn());
                        }
                        startPs.close();
                        endPs.close();
                    }
                }
                JSONObject fieldMapper = this.sqlCdcConfig.getDirectSinkConfig().getJSONObject("field_mapper");
                List<String> sourceColumns= new ArrayList<>();
                fieldMapper.forEach((key,value)->sourceColumns.add(key));
                String newSql = this.jdbcDialect.getPartitionSql(this.sqlCdcConfig.getPartitionColumn(), this.sqlCdcConfig.getQuery(),Optional.of(sourceColumns));
                PreparedStatement ps = conn.prepareStatement(newSql);
                ps.setObject(1, startRowNumber);
                ps.setObject(2, endRowNumber);
                ps.setFetchSize(10000);
                ps.executeQuery();
                ResultSet resultSet = ps.getResultSet();
                while (resultSet.next()) {
                    SeaTunnelRow seaTunnelRowInsert = jdbcDialect.getRowConverter().toInternal(resultSet, typeInfo);
                    List<String> keys = new ArrayList<>();
                    for (String primaryKey : this.sqlCdcConfig.getPrimaryKeys()) {
                        Integer i = mapPosition.get(primaryKey);
                        Object field = seaTunnelRowInsert.getField(i);
                        keys.add(field.toString());
                    }
                }
                ps.close();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
    }
    private void normalCompare(Collector<SeaTunnelRow> output){
        try {
            conn.setAutoCommit(false);
            if (sqlCdcConfig.getDbType().equalsIgnoreCase("mysql")) {
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
            Map<String, Integer> mapPosition = new HashMap<>();
            for (String primaryKey : this.sqlCdcConfig.getPrimaryKeys()) {
                int i = typeInfo.indexOf(primaryKey);
                mapPosition.put(primaryKey, i);
            }
            Map<String, java.util.Date> map = new HashMap<>();
            while (true) {
                String partitionColumnCountSql = this.jdbcDialect.getColumnDistinctCount(this.sqlCdcConfig.getPartitionColumn(), this.sqlCdcConfig);
                PreparedStatement psTotal = conn.prepareStatement(partitionColumnCountSql);
                ResultSet resultSet1 = psTotal.executeQuery();
                long totalSl = 0;
                int interval = 0;
                while (resultSet1.next()) {
                    totalSl = resultSet1.getLong("sl");
                    interval = (int) Math.ceil((double) totalSl / this.allTask);
                }
                psTotal.close();
                for (int i = 0; i < allTask; i++) {
                    if (i == currentTaskId) {
                        long startHang = i * interval + 1;
                        long endHang = (i + 1) * interval;
                        if (endHang > totalSl) {
                            endHang = totalSl;
                        }
                        PreparedStatement startPs = conn.prepareStatement(this.jdbcDialect.getHangValueSql(this.sqlCdcConfig, startHang));
                        ResultSet rsStart = startPs.executeQuery();
                        while (rsStart.next()) {
                            startRowNumber = rsStart.getObject(this.sqlCdcConfig.getPartitionColumn());
                        }
                        PreparedStatement endPs = conn.prepareStatement(this.jdbcDialect.getHangValueSql(this.sqlCdcConfig, endHang));
                        ResultSet rsEnd = endPs.executeQuery();
                        while (rsEnd.next()) {
                            endRowNumber = rsEnd.getObject(this.sqlCdcConfig.getPartitionColumn());
                        }
                        startPs.close();
                        endPs.close();
                    }
                }
                String newSql = this.jdbcDialect.getPartitionSql(this.sqlCdcConfig.getPartitionColumn(), this.sqlCdcConfig.getQuery(),Optional.empty());
                Date versionDate = new Date();
                PreparedStatement ps = conn.prepareStatement(newSql);
                ps.setObject(1, startRowNumber);
                ps.setObject(2, endRowNumber);
                ps.setFetchSize(10000);
                ps.executeQuery();
                ResultSet resultSet = ps.getResultSet();
                while (resultSet.next()) {
                    SeaTunnelRow seaTunnelRowInsert = jdbcDialect.getRowConverter().toInternal(resultSet, typeInfo);
                    List<String> keys = new ArrayList<>();
                    for (String primaryKey : this.sqlCdcConfig.getPrimaryKeys()) {
                        Integer i = mapPosition.get(primaryKey);
                        Object field = seaTunnelRowInsert.getField(i);
                        keys.add(field.toString());
                    }
                    keys.add(String.valueOf(seaTunnelRowInsert.hashCode()));
                    seaTunnelRowInsert.setRowKind(RowKind.INSERT);
                    SeaTunnelRow seaTunnelRowDelete=seaTunnelRowInsert.copy();
                    seaTunnelRowDelete.setRowKind(RowKind.DELETE);
                    if (!map.containsKey(StringUtils.join(keys, ","))) {
                        output.collect(seaTunnelRowDelete);
                        output.collect(seaTunnelRowInsert);
                        String join = StringUtils.join(keys, ",");
                        int lastCommaIndex = join.lastIndexOf(",");
                        String prefix = join.substring(0, lastCommaIndex);
                        Iterator<Map.Entry<String, Date>> iterator1 = map.entrySet().iterator();
                        while (iterator1.hasNext()) {
                            Map.Entry<String, Date> entry = iterator1.next();
                            if (entry.getKey().startsWith(prefix)) {
                                iterator1.remove();
                            }
                        }
                    }
                    map.put(StringUtils.join(keys, ","), versionDate);
                }
                Iterator<Map.Entry<String, Date>> iterator = map.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Date> entry = iterator.next();
                    if (!versionDate.equals(entry.getValue())) checkDelete(output, entry, mapPosition, iterator);
                }
                ps.close();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
    }
}
