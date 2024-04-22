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

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
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
    private final SeaTunnelRowType typeInfoOperateflagOperatetime;

    SqlCdcReader(SqlCdcConfig sqlCdcConfig, SourceReader.Context context, List<ColumnMapper> columnMappers, SeaTunnelRowType typeInfo) {
        this.sqlCdcConfig = sqlCdcConfig;
        this.context = context;
        this.columnMappers = columnMappers;
        this.typeInfoOperateflagOperatetime = typeInfo;
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
                    String sql;
                    if (sqlCdcConfig.getRecordOperation()) {
                        sql = String.format("select  %s from (%s) a where OPERATEFLAG in ('I','U')", StringUtils.join(sinkColumns, ","), sinkJdbcConfig.getQuery());
                    } else {
                        sql = String.format("select  %s from (%s) a ", StringUtils.join(sinkColumns, ","), sinkJdbcConfig.getQuery());
                    }

                    return getTableColumnsHaseCode(sql, "sink", sinkJdbcConfig);
                }
            });
            Thread threadSource = new Thread(sourceFuture);
            Thread threadSink = new Thread(futureSink);
            threadSource.start();
            threadSink.start();
            Map<String, Integer> resultSource = sourceFuture.get();
            Map<String, Integer> resultSink = futureSink.get();
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String formattedString = now.format(formatter);
            MapDifference<String, Object> difference = Maps.difference(resultSource, resultSink);
            Map<String, MapDifference.ValueDifference<Object>> entriesDiffering = difference.entriesDiffering();
            doModify(entriesDiffering.keySet(), "U", output, formattedString);
            Map<String, Object> onlyOnLeft = difference.entriesOnlyOnLeft();
            doModify(onlyOnLeft.keySet(), "I", output, formattedString);
            Map<String, Object> entriesOnlyOnRight = difference.entriesOnlyOnRight();
            doModify(entriesOnlyOnRight.keySet(), "D", output, formattedString);
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

    private void doModify(Set<String> pksValues, String doFlag, Collector<SeaTunnelRow> output, String modifyTime) throws SQLException {
        List<String> list = new ArrayList<>(pksValues);
        List<List<String>> partition = Lists.partition(list, 200);
        Util util = new Util();
        Connection conn = util.getConnection(sourceJdbcConfig);
        JdbcDialect jdbcDialect = JdbcDialectFactory.getJdbcDialect(sourceJdbcConfig.getDbType());
        SeaTunnelRowType dataTypeInfo = getTypeInfo(conn, jdbcDialect, sourceJdbcConfig.getQuery(), columnMappers.stream().map(ColumnMapper::getSourceColumnName).collect(Collectors.toList()));
        SeaTunnelRowType typeInfo;
        if (this.sqlCdcConfig.getRecordOperation()) {
            typeInfo = typeInfoOperateflagOperatetime;
        } else {
            typeInfo = dataTypeInfo;
        }
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
                    SeaTunnelRow seaTunnelRowInsert = jdbcDialect.getRowConverter().toInternal(resultSet, dataTypeInfo);
                    seaTunnelRowInsert.setRowKind(RowKind.INSERT);
                    SeaTunnelRow seaTunnelRowDelete = seaTunnelRowInsert.copy();
                    seaTunnelRowDelete.setRowKind(RowKind.DELETE);
                    if (this.sqlCdcConfig.getRecordOperation()) {
                        SeaTunnelRow newInsert = new SeaTunnelRow(typeInfoOperateflagOperatetime.getTotalFields());
                        newInsert.setRowKind(RowKind.INSERT);
                        SeaTunnelRow newDelete = new SeaTunnelRow(typeInfoOperateflagOperatetime.getTotalFields());
                        newDelete.setRowKind(RowKind.DELETE);
                        Object[] fields = seaTunnelRowInsert.getFields();
                        for (int i = 0; i < fields.length; i++) {
                            newInsert.setField(i, fields[i]);
                            newDelete.setField(i, fields[i]);
                        }
                        if (doFlag.equalsIgnoreCase("I")) {
                            newInsert.setField(fields.length, "I");
                        } else {
                            newInsert.setField(fields.length, "U");
                        }
                        newInsert.setField(fields.length + 1, modifyTime);
                        output.collect(newDelete);
                        output.collect(newInsert);

                    } else {
                        output.collect(seaTunnelRowDelete);
                        output.collect(seaTunnelRowInsert);
                    }


                }
                ps.close();
            }

        } else if (doFlag.equalsIgnoreCase("D")) {
            deleteSinkData(output, modifyTime, partition, typeInfo);
        }
        conn.close();
    }

    private void deleteSinkData(Collector<SeaTunnelRow> output, String modifyTime, List<List<String>> partition, SeaTunnelRowType typeInfo) throws SQLException {
        Util util = new Util();
        Connection conn = util.getConnection(sinkJdbcConfig);
        JdbcDialect jdbcDialect = JdbcDialectFactory.getJdbcDialect(sinkJdbcConfig.getDbType());
        List<String> collect = columnMappers.stream().filter(ColumnMapper::isPrimaryKey).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> sinkColumns = new ArrayList<>();
        columnMappers.forEach(x -> sinkColumns.add(x.getSourceColumnName()));
        String sql;
        SeaTunnelRowType dataTypeInfo;
        if (sqlCdcConfig.getRecordOperation()) {
            String[] fieldNames = typeInfo.getFieldNames();
            SeaTunnelDataType<?>[] fieldTypes = typeInfo.getFieldTypes();
            dataTypeInfo = new SeaTunnelRowType(Arrays.copyOfRange(fieldNames, 0, fieldNames.length - 2), Arrays.copyOfRange(fieldTypes, 0, fieldTypes.length - 2));
            sql = String.format("select  %s from (%s) a where 1=1 and OPERATEFLAG in ('I','U') ", StringUtils.join(sinkColumns, ","), sinkJdbcConfig.getQuery());
        } else {
            dataTypeInfo = typeInfo;
            sql = String.format("select  %s from (%s) a where 1=1 ", StringUtils.join(sinkColumns, ","), sinkJdbcConfig.getQuery());
        }
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
            PreparedStatement ps = conn.prepareStatement(String.format("%s and  ( %s )", sql, where));
            ps.setFetchSize(200);
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();
            while (resultSet.next()) {
                SeaTunnelRow seaTunnelSinkData = jdbcDialect.getRowConverter().toInternal(resultSet, dataTypeInfo);
                Object[] sinkFields = seaTunnelSinkData.getFields();
                for (String ignored : pksValue1) {
                    if (this.sqlCdcConfig.getRecordOperation()) {
                        SeaTunnelRow newInsert = new SeaTunnelRow(typeInfo.getTotalFields());
                        newInsert.setRowKind(RowKind.INSERT);
                        SeaTunnelRow newDelete = new SeaTunnelRow(typeInfo.getTotalFields());
                        newDelete.setRowKind(RowKind.DELETE);
                        for (int i = 0; i < sinkFields.length; i++) {
                            newInsert.setField(i, sinkFields[i]);
                            newDelete.setField(i, sinkFields[i]);
                        }
                        newInsert.setField(sinkFields.length, "D");
                        newInsert.setField(sinkFields.length + 1, modifyTime);
                        output.collect(newDelete);
                        output.collect(newInsert);
                    } else {
                        SeaTunnelRow seaTunnelRowDelete = new SeaTunnelRow(typeInfo.getTotalFields());
                        for (int i = 0; i < sinkFields.length; i++) {
                            seaTunnelRowDelete.setField(i, sinkFields[i]);
                            seaTunnelRowDelete.setField(i, sinkFields[i]);
                        }
                        seaTunnelRowDelete.setRowKind(RowKind.DELETE);
                        output.collect(seaTunnelRowDelete);
                    }

                }

            }
        }
        conn.close();
    }

}
