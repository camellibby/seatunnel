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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qh.sqlcdc.config.ColumnMapper;
import com.qh.sqlcdc.config.JdbcConfig;
import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.*;
import com.google.common.collect.MapDifference;
import org.apache.seatunnel.common.exception.CommonErrorCode;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

@Slf4j
public class SqlCdcReader implements SourceReader<SeaTunnelRow, SqlCdcSourceSplit> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SourceReader.Context context;

    private final List<ColumnMapper> columnMappers;

    private final JdbcConfig sourceJdbcConfig;
    private final JdbcConfig sinkJdbcConfig;
    private final List<Integer> keysIndex = new ArrayList<>();
    ;

    SqlCdcReader(SqlCdcConfig sqlCdcConfig, SourceReader.Context context, List<ColumnMapper> columnMappers) {
        this.sqlCdcConfig = sqlCdcConfig;
        this.context = context;
        this.columnMappers = columnMappers;
        {
            JdbcConfig jdbcConfig = new JdbcConfig();
            jdbcConfig.setUser(sqlCdcConfig.getUser());
            jdbcConfig.setPassWord(sqlCdcConfig.getPassWord());
            jdbcConfig.setUrl(sqlCdcConfig.getUrl());
            jdbcConfig.setDbType(sqlCdcConfig.getDbType());
            jdbcConfig.setQuery(sqlCdcConfig.getQuery());
            jdbcConfig.setDriver(sqlCdcConfig.getDriver());
            this.sourceJdbcConfig = jdbcConfig;
        }
        {
            JdbcConfig jdbcConfig = new JdbcConfig();
            JSONObject sinkConfig = sqlCdcConfig.getDirectSinkConfig();
            jdbcConfig.setUser(sinkConfig.getString("user"));
            jdbcConfig.setPassWord(sinkConfig.getString("password"));
            jdbcConfig.setUrl(sinkConfig.getString("url"));
            jdbcConfig.setDbType(sinkConfig.getString("db_type"));
            jdbcConfig.setQuery(String.format("select  * from %s ", sinkConfig.getString("table")));
            jdbcConfig.setDriver(sinkConfig.getString("driver"));
            this.sinkJdbcConfig = jdbcConfig;
        }
        {
            for (int i = 0; i < columnMappers.size(); i++) {
                if (columnMappers.get(i).isPrimaryKey()) {
                    keysIndex.add(i);
                }
            }
        }


    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        compare(output);
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

    private void compare(Collector<SeaTunnelRow> output) throws ExecutionException, InterruptedException, SQLException {
        do {
            FutureTask<Map<String, Integer>> sourceFuture = new FutureTask<>(new Callable<Map<String, Integer>>() {
                @Override
                public Map<String, Integer> call() throws Exception {
                    List<String> sourceColumns = new ArrayList<>();
                    columnMappers.forEach(x -> sourceColumns.add(x.getSourceColumnName()));
                    String sql = String.format("select  %s from (%s) a ", StringUtils.join(sourceColumns, ","), sqlCdcConfig.getQuery());
                    return getTableColumnsHaseCode(sql, "source", sourceJdbcConfig);
                }
            });

            FutureTask<Map<String, Integer>> futureSink = new FutureTask<>(new Callable<Map<String, Integer>>() {
                @Override
                public Map<String, Integer> call() throws Exception {
                    List<String> sinkColumns = new ArrayList<>();
                    columnMappers.forEach(x -> sinkColumns.add(x.getSinkColumnName()));
                    String sql = String.format("select  %s from (%s) a ", StringUtils.join(sinkColumns, ","), sinkJdbcConfig.getQuery());
                    return getTableColumnsHaseCode(sql, "sink", sinkJdbcConfig);
                }
            });
            Thread threadSource = new Thread(sourceFuture);
            Thread threadSink = new Thread(futureSink);
            threadSource.start();
            threadSink.start();
            Map<String, Integer> resultSource = sourceFuture.get();
            Map<String, Integer> resultSink = futureSink.get();
            MapDifference<String, Object> difference = Maps.difference(resultSource, resultSink);
            Map<String, MapDifference.ValueDifference<Object>> entriesDiffering = difference.entriesDiffering();
            doModify(entriesDiffering.keySet(), "U", output);
            Map<String, Object> onlyOnLeft = difference.entriesOnlyOnLeft();
            doModify(onlyOnLeft.keySet(), "I", output);
            Map<String, Object> entriesOnlyOnRight = difference.entriesOnlyOnRight();
            doModify(entriesOnlyOnRight.keySet(), "D", output);
        } while (true);
    }


    private SeaTunnelRowType getTypeInfo(Connection conn, JdbcDialect jdbcDialect, String sql, List<String> columns) {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            sql = String.format("select %s from (%s) a where 1=2", StringUtils.join(columns, ","), sql);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeQuery();
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
            ps.close();
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[0]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private Map<String, Integer> getTableColumnsHaseCode(String sql, String tableFlag, JdbcConfig jdbcConfig) throws SQLException {
        Util util = new Util();
        Connection conn = util.getConnection(jdbcConfig);
        JdbcDialect jdbcDialect = JdbcDialectFactory.getJdbcDialect(jdbcConfig.getDbType());
        Map<String, Integer> map = new HashMap<>();
        SeaTunnelRowType typeInfo = null;
        if (tableFlag.equalsIgnoreCase("source")) {
            typeInfo = getTypeInfo(conn, jdbcDialect, sql, columnMappers.stream().map(ColumnMapper::getSourceColumnName).collect(Collectors.toList()));
        }
        if (tableFlag.equalsIgnoreCase("sink")) {
            typeInfo = getTypeInfo(conn, jdbcDialect, sql, columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
        }
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setFetchSize(10000);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        while (resultSet.next()) {
            SeaTunnelRow seaTunnelRow = jdbcDialect.getRowConverter().toInternal(resultSet, typeInfo);
            SeaTunnelRow newRow = new SeaTunnelRow(columnMappers.size());
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < columnMappers.size(); i++) {
                Object field = seaTunnelRow.getField(i);
                newRow.setField(i, util.Object2String(field));
                if (keysIndex.contains(i)) {
                    keys.add(util.Object2String(field));
                }
            }
            map.put(StringUtils.join(keys, ","), Arrays.deepHashCode(newRow.getFields()));
        }
        ps.close();
        conn.close();
        return map;
    }

    private void doModify(Set<String> pksValues, String doFlag, Collector<SeaTunnelRow> output) throws SQLException {
        List<String> list = new ArrayList<>(pksValues);
        List<List<String>> partition = Lists.partition(list, 200);
        Util util = new Util();
        Connection conn = util.getConnection(sourceJdbcConfig);
        JdbcDialect jdbcDialect = JdbcDialectFactory.getJdbcDialect(sourceJdbcConfig.getDbType());
        SeaTunnelRowType typeInfo = getTypeInfo(conn, jdbcDialect, sourceJdbcConfig.getQuery(), columnMappers.stream().map(ColumnMapper::getSourceColumnName).collect(Collectors.toList()));
        List<String> collect = columnMappers.stream().filter(ColumnMapper::isPrimaryKey).map(ColumnMapper::getSourceColumnName).collect(Collectors.toList());
        List<String> sourceColumns = new ArrayList<>();
        columnMappers.forEach(x -> sourceColumns.add(x.getSourceColumnName()));
        String sql = String.format("select  %s from (%s) a ", StringUtils.join(sourceColumns, ","), sourceJdbcConfig.getQuery());
        if (doFlag.equalsIgnoreCase("I") || doFlag.equalsIgnoreCase("U")) {
            for (List<String> pksValue1 : partition) {
                List<String> replaceAll = new ArrayList<>();
                for (String pksValue2 : pksValue1) {
                    String[] split = pksValue2.split(",");
                    List<String> columns = new ArrayList<>();
                    for (int i = 0; i < split.length; i++) {
                        String column = String.format("%s='%s'", collect.get(i), split[i]);
                        columns.add(column);
                    }
                    replaceAll.add("(" + StringUtils.join(columns, " and ") + ")");
                }
                String where = StringUtils.join(replaceAll, " or ");
                PreparedStatement ps = conn.prepareStatement(String.format("%s where %s", sql, where));
                ps.setFetchSize(200);
                ps.executeQuery();
                ResultSet resultSet = ps.getResultSet();
                while (resultSet.next()) {
                    SeaTunnelRow seaTunnelRowInsert = jdbcDialect.getRowConverter().toInternal(resultSet, typeInfo);
                    seaTunnelRowInsert.setRowKind(RowKind.INSERT);
                    SeaTunnelRow seaTunnelRowDelete = seaTunnelRowInsert.copy();
                    seaTunnelRowDelete.setRowKind(RowKind.DELETE);
                    output.collect(seaTunnelRowDelete);
                    output.collect(seaTunnelRowInsert);
                }
                ps.close();
            }

        } else if (doFlag.equalsIgnoreCase("D")) {
            for (List<String> pksValue1 : partition) {
                SeaTunnelRow seaTunnelRowDelete = new SeaTunnelRow(typeInfo.getTotalFields());
                seaTunnelRowDelete.setRowKind(RowKind.DELETE);
                for (String pksValue2 : pksValue1) {
                    String[] split = pksValue2.split(",");
                    for (int i = 0; i < this.sqlCdcConfig.getPrimaryKeys().size(); i++) {
                        Integer position = keysIndex.get(i);
                        SeaTunnelDataType<?> fieldType = typeInfo.getFieldType(position);
                        SqlType sqlType = fieldType.getSqlType();
                        switch (sqlType) {
                            case STRING:
                                seaTunnelRowDelete.setField(position, split[i]);
                                break;
                            case BOOLEAN:
                                seaTunnelRowDelete.setField(position, Boolean.parseBoolean(split[i]));
                                break;
                            case TINYINT:
                                seaTunnelRowDelete.setField(position, Integer.parseInt(split[i]));
                                break;
                            case SMALLINT:
                                seaTunnelRowDelete.setField(position, Integer.parseInt(split[i]));
                                break;
                            case INT:
                                seaTunnelRowDelete.setField(position, Integer.parseInt(split[i]));
                                break;
                            case BIGINT:
                                seaTunnelRowDelete.setField(position, Long.parseLong(split[i]));
                                break;
                            case FLOAT:
                                seaTunnelRowDelete.setField(position, Float.parseFloat(split[i]));
                                break;
                            case DOUBLE:
                                seaTunnelRowDelete.setField(position, Double.parseDouble(split[i]));
                                break;
                            case DECIMAL:
                                seaTunnelRowDelete.setField(position, new BigDecimal(split[i]));
                                break;
                            case DATE:
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                Date date = null;
                                try {
                                    date = sdf.parse(split[i]);
                                } catch (ParseException e) {
                                    throw new RuntimeException(e);
                                }
                                java.sql.Date sqlDate = new java.sql.Date(date.getTime());
                                seaTunnelRowDelete.setField(position, sqlDate);
                                break;
                            case TIME:
                                SimpleDateFormat sdf1 = new SimpleDateFormat("HH:mm:ss");
                                Date date1 = null;
                                try {
                                    date1 = sdf1.parse(split[i]);
                                } catch (ParseException e) {
                                    throw new RuntimeException(e);
                                }
                                Time sqlTime = new Time(date1.getTime());
                                seaTunnelRowDelete.setField(position, sqlTime);
                                break;
                            case TIMESTAMP:
                                String dateStr = split[i];
                                SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                Timestamp timestamp = null;
                                try {
                                    timestamp = new Timestamp(sdf3.parse(dateStr).getTime());
                                } catch (ParseException e) {
                                    throw new RuntimeException(e);
                                }
                                seaTunnelRowDelete.setField(position, timestamp);
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
                    output.collect(seaTunnelRowDelete);
                }
            }
        }
        conn.close();
    }

}
