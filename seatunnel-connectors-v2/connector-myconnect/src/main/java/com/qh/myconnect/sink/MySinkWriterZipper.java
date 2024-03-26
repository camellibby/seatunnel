package com.qh.myconnect.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.config.SeaTunnelJobsHistoryErrorRecord;
import com.qh.myconnect.config.StatisticalLog;
import com.qh.myconnect.config.Util;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;


@Slf4j
public class MySinkWriterZipper extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private SeaTunnelRowType sinkTableRowType;
    private List<SeaTunnelRow> cld = new ArrayList<>();
    private Long writeCount = 0L;
    private Long keepCount = 0L;
    private Long updateCount = 0L;
    private Long deleteCount = 0L;
    private Long insertCount = 0L;
    private Long errorCount = 0L;

    private final JdbcSinkConfig jdbcSinkConfig;
    private JobContext jobContext;
    private LocalDateTime startTime;
    private int batchSize = 2000;
    private JdbcDialect jdbcDialect;
    private String table;
    private String tmpTable;

    private Map<String, String> metaDataHash;

    private Connection conn;

    private List<ColumnMapper> columnMappers = new ArrayList<>();
    private final List<String> zipperColumns = Arrays.asList("OPERATEFLAG", "OPERATETIME");

    private final Util util = new Util();

    private Set sqlErrorType = new HashSet();
    private final Integer currentTaskId;

    public MySinkWriterZipper(SeaTunnelRowType seaTunnelRowType,
                              Context context,
                              ReadonlyConfig config,
                              JobContext jobContext,
                              LocalDateTime startTime) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.startTime = startTime;
        this.currentTaskId = context.getIndexOfSubtask();
        this.table = this.jdbcSinkConfig.getTable();
        this.tmpTable = "UC_" + this.jdbcSinkConfig.getTable();
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
        conn.setAutoCommit(false);
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);

        String sqlQuery = jdbcDialect.getSinkQueryZipper(this.columnMappers, 0, jdbcSinkConfig);
        PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
        ResultSet resultSet = preparedStatementQuery.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> metaDataHash = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            metaDataHash.put(metaData.getColumnName(i + 1), metaData.getColumnTypeName(i + 1));
        }
        this.metaDataHash = metaDataHash;
        preparedStatementQuery.close();
    }

    private void consumeData() {
        HashMap<List<String>, SeaTunnelRow> sourceRows = new HashMap<>();
        List<ColumnMapper> ucColumns = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        for (SeaTunnelRow seaTunnelRow : cld) {
            if (seaTunnelRow != null) {
                SeaTunnelRow newRow = new SeaTunnelRow(columnMappers.size());
                for (int i = 0; i < this.columnMappers.size(); i++) {
                    Object field = seaTunnelRow.getField(this.columnMappers.get(i).getSourceRowPosition());
                    newRow.setField(i, util.Object2String(field));
                }
                List<String> keys = new ArrayList<>();
                ucColumns.forEach(x -> {
                    Object field = seaTunnelRow.getField(x.getSourceRowPosition());
                    keys.add(util.Object2String(field));
                });
                sourceRows.put(keys, newRow);
            }

        }
        try {
            insertTmpUks(this.cld, this.metaDataHash, conn);
            compareData(sourceRows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void compareData(HashMap<List<String>, SeaTunnelRow> sourceRows) throws Exception {
        HashMap<List<String>, SeaTunnelRow> sinkRows = getSinkRows(conn, sourceRows);
        List<SeaTunnelRow> needInsertRows = new ArrayList<>();
        HashMap<List<String>, SeaTunnelRow> needUpdate = new HashMap<>();
        sourceRows.forEach((k, sourceRow) -> {
            SeaTunnelRow sinkRow = sinkRows.get(k);
            if (null != sinkRow) {
                if (!sourceRow.equals(sinkRow)) {
                    needUpdate.put(k, sourceRow);
                } else {
                    this.keepCount++;
                }
            } else {
                needInsertRows.add(sourceRow);
            }
        });
        doInsert(needInsertRows, this.metaDataHash, conn);
        doUpdate(needUpdate, this.metaDataHash, conn);

    }

    private void initColumnMappers(JdbcSinkConfig jdbcSinkConfig, SeaTunnelRowType sourceRowType, SeaTunnelRowType sinkTableRowType, Connection conn) {
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        fieldMapper.forEach((k, v) -> {
            ColumnMapper columnMapper = new ColumnMapper();
            columnMapper.setSourceColumnName(k);
            columnMapper.setSourceRowPosition(sourceRowType.indexOf(k));
            String typeNameSS = sourceRowType.getFieldType(sourceRowType.indexOf(k)).getTypeClass().getName();
            columnMapper.setSourceColumnTypeName(typeNameSS);

            columnMapper.setSinkColumnName(v);
            columnMapper.setSinkRowPosition(sinkTableRowType.indexOf(v));
            String typeNameSK = sinkTableRowType.getFieldType(sinkTableRowType.indexOf(v)).getTypeClass().getName();
            columnMapper.setSinkColumnTypeName(typeNameSK);

            for (String primaryKey : jdbcSinkConfig.getPrimaryKeys()) {
                if (primaryKey.equalsIgnoreCase(v)) {
                    columnMapper.setUc(true);
                }
            }
            try {
                ResultSetMetaData metaData = this.jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    if (v.equalsIgnoreCase(columnName)) {
                        String columnTypeName = metaData.getColumnTypeName(i + 1);
                        columnMapper.setSinkColumnDbType(columnTypeName);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            columnMappers.add(columnMapper);
        });
    }

    @Override
    public void write(SeaTunnelRow element) {
        this.writeCount++;
        this.cld.add(element);
        if (this.writeCount.longValue() % batchSize == 0) {
            this.consumeData();
            cld.clear();
        }
    }

    private void doInsert(List<SeaTunnelRow> rows, Map<String, String> metaDataHash, Connection connection) throws Exception {
        Long tmpInsertCount = null;
        try {
            List<String> newColumns = new ArrayList<>();
            List<String> newValues = new ArrayList<>();
            for (ColumnMapper column : columnMappers) {
                newColumns.add(column.getSinkColumnName());
            }
            newColumns.addAll(this.zipperColumns);
            for (String newColumn : newColumns) {
                newValues.add("?");
            }
            String insertSql = jdbcDialect.insertTableSql(jdbcSinkConfig, newColumns, newValues);
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            tmpInsertCount = this.insertCount;
            boolean hasError = false;
            for (SeaTunnelRow row : rows) {
                for (int i = 0; i < columnMappers.size(); i++) {
                    String column = newColumns.get(i);
                    String dbType = metaDataHash.get(column);
                    jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, (String) row.getField(i));
                }
                jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 1, preparedStatement, "VARCHAR2", "I");
                jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 2, preparedStatement, "VARCHAR2", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                this.insertCount++;
                try {
                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    hasError = true;
                    break;
                }
            }
            if (hasError) {
                throw new RuntimeException();
            }
            preparedStatement.executeBatch();
            conn.commit();
            preparedStatement.clearBatch();
            preparedStatement.close();
        } catch (Exception e) {
            conn.rollback();
            this.insertCount = tmpInsertCount;
            doInsertOneByOne(rows);
        }
    }

    private void doInsertOneByOne(List<SeaTunnelRow> rows) throws Exception {
        List<String> newColumns = new ArrayList<>();
        List<String> newValues = new ArrayList<>();
        for (ColumnMapper column : columnMappers) {
            newColumns.add(column.getSinkColumnName());
        }
        newColumns.addAll(this.zipperColumns);
        for (String newColumn : newColumns) {
            newValues.add("?");
        }
        String insertSql = jdbcDialect.insertTableSql(jdbcSinkConfig, newColumns, newValues);
        for (SeaTunnelRow row : rows) {
            PreparedStatement preparedStatement = conn.prepareStatement(insertSql);
            for (int i = 0; i < columnMappers.size(); i++) {
                String column = newColumns.get(i);
                String dbType = metaDataHash.get(column);
                jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, (String) row.getField(i));
            }
            jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 1, preparedStatement, "VARCHAR2", "I");
            jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 2, preparedStatement, "VARCHAR2", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            try {
                preparedStatement.addBatch();
                preparedStatement.executeBatch();
                conn.commit();
                preparedStatement.clearBatch();
                this.insertCount++;
                preparedStatement.close();
            } catch (SQLException e) {
                this.errorCount++;
                if (this.jobContext.getIsRecordErrorData() == 1 && this.errorCount <= this.jobContext.getMaxRecordNumber() && !sqlErrorType.contains(e.getMessage())) {
                    JSONObject jsonObject = new JSONObject();
                    for (int i = 0; i < sinkTableRowType.getTotalFields(); i++) {
                        jsonObject.put(sinkTableRowType.getFieldName(i), row.getField(i));
                    }
                    log.info(JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue));
                    SeaTunnelJobsHistoryErrorRecord errorRecord = new SeaTunnelJobsHistoryErrorRecord();
                    errorRecord.setFlinkJobId(this.jobContext.getJobId());
                    errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                    errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                    errorRecord.setTableName(jdbcSinkConfig.getTable());
                    errorRecord.setErrorData(JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue));
                    errorRecord.setErrorMessage(e.getMessage());
                    sqlErrorType.add(e.getMessage());
                    try {
                        util.insertErrorData(errorRecord);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    private void doUpdate(HashMap<List<String>, SeaTunnelRow> rows, Map<String, String> metaDataHash, Connection connection) throws Exception {
        Long tmpUpdateCount = null;
        try {
            List<String> newColumns = new ArrayList<>();
            List<String> newValues = new ArrayList<>();
            for (ColumnMapper column : columnMappers) {
                newColumns.add(column.getSinkColumnName());
            }
            newColumns.addAll(this.zipperColumns);
            for (String newColumn : newColumns) {
                newValues.add("?");
            }
            String insertSql = jdbcDialect.insertTableSql(jdbcSinkConfig, newColumns, newValues);
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            tmpUpdateCount = this.updateCount;
            boolean hasError = false;
            for (SeaTunnelRow row : rows.values()) {
                for (int i = 0; i < columnMappers.size(); i++) {
                    String column = newColumns.get(i);
                    String dbType = metaDataHash.get(column);
                    jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, (String) row.getField(i));
                }
                jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 1, preparedStatement, "VARCHAR2", "U");
                jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 2, preparedStatement, "VARCHAR2", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                try {
                    preparedStatement.addBatch();
                    this.updateCount++;
                } catch (SQLException e) {
                    hasError = true;
                    break;
                }
            }
            if (hasError) {
                throw new RuntimeException();
            }
            preparedStatement.executeBatch();
            conn.commit();
            preparedStatement.clearBatch();
            preparedStatement.close();
        } catch (Exception e) {
            conn.rollback();
            this.updateCount = tmpUpdateCount;
            updateDbOneByOne(rows);
        }
    }

    private void updateDbOneByOne(HashMap<List<String>, SeaTunnelRow> rows) {
        try {
            List<String> newColumns = new ArrayList<>();
            List<String> newValues = new ArrayList<>();
            for (ColumnMapper column : columnMappers) {
                newColumns.add(column.getSinkColumnName());
            }
            newColumns.addAll(this.zipperColumns);
            for (String newColumn : newColumns) {
                newValues.add("?");
            }
            String insertSql = jdbcDialect.insertTableSql(jdbcSinkConfig, newColumns, newValues);
            for (SeaTunnelRow row : rows.values()) {
                PreparedStatement preparedStatement = conn.prepareStatement(insertSql);
                for (int i = 0; i < columnMappers.size(); i++) {
                    String column = newColumns.get(i);
                    String dbType = metaDataHash.get(column);
                    jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, (String) row.getField(i));
                }
                jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 1, preparedStatement, "VARCHAR2", "U");
                jdbcDialect.setPreparedStatementValueByDbType(columnMappers.size() + 2, preparedStatement, "VARCHAR2", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                try {
                    preparedStatement.addBatch();
                    this.updateCount++;
                    preparedStatement.executeBatch();
                    conn.commit();
                    preparedStatement.clearBatch();
                    preparedStatement.close();
                } catch (SQLException e) {
                    this.errorCount++;
                    if (this.jobContext.getIsRecordErrorData() == 1 && this.errorCount <= this.jobContext.getMaxRecordNumber() && !sqlErrorType.contains(e.getMessage())) {
                        JSONObject jsonObject = new JSONObject();
                        for (int i = 0; i < sinkTableRowType.getTotalFields(); i++) {
                            jsonObject.put(sinkTableRowType.getFieldName(i), row.getField(i));
                        }
                        log.info(JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue));
                        SeaTunnelJobsHistoryErrorRecord errorRecord = new SeaTunnelJobsHistoryErrorRecord();
                        errorRecord.setFlinkJobId(this.jobContext.getJobId());
                        errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                        errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                        errorRecord.setTableName(jdbcSinkConfig.getTable());
                        errorRecord.setErrorData(JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue));
                        errorRecord.setErrorMessage(e.getMessage());
                        sqlErrorType.add(e.getMessage());
                        try {
                            util.insertErrorData(errorRecord);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


    private void insertTmpUks(List<SeaTunnelRow> oldRows, Map<String, String> metaDataHash, Connection conn) throws SQLException {
        List<ColumnMapper> ucColumns = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        JdbcSinkConfig newJdbcSinkConfig = JdbcSinkConfig.builder()
                .dbSchema(jdbcSinkConfig.getDbSchema())
                .table(tmpTable)
                .build();
        String insert = jdbcDialect.insertTableSql(
                newJdbcSinkConfig,
                ucColumns.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList()),
                ucColumns.stream().map(x -> "?").collect(Collectors.toList())
        );
        PreparedStatement preparedStatement = conn.prepareStatement(insert);
        for (SeaTunnelRow oldRow : oldRows) {
            for (int i = 0; i < ucColumns.size(); i++) {
                String dbType = metaDataHash.get(ucColumns.get(i).getSinkColumnName());
                jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, util.Object2String(oldRow.getField(ucColumns.get(i).getSourceRowPosition())));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
        conn.commit();
    }

    private HashMap<List<String>, SeaTunnelRow> getSinkRows(Connection conn, HashMap<List<String>, SeaTunnelRow> sourceRows) {
        AtomicInteger tmp = new AtomicInteger();
        HashMap<List<String>, SeaTunnelRow> sinkRows = new HashMap<>();
        String sqlQuery = jdbcDialect.getSinkQueryZipper(this.columnMappers, sourceRows.size(), jdbcSinkConfig);
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        try {
            PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
            sourceRows.forEach((k, v) -> {
                for (int i = 0; i < ucColumns.size(); i++) {
                    try {
                        jdbcDialect.setPreparedStatementValue(preparedStatementQuery, (i + 1) + tmp.get() * ucColumns.size(), k.get(i));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                tmp.getAndIncrement();
            });
            ResultSet resultSet = preparedStatementQuery.executeQuery();
            while (resultSet.next()) {
                SeaTunnelRow internal = util.toInternal(resultSet, this.sinkTableRowType);
                SeaTunnelRow newRow = new SeaTunnelRow(columnMappers.size());
                for (int i = 0; i < this.columnMappers.size(); i++) {
                    Object field = internal.getField(columnMappers.get(i).getSinkRowPosition());
                    newRow.setField(i, util.Object2String(field));
                }
                List<String> keys = new ArrayList<>();
                ucColumns.forEach((x -> {
                    Object field = internal.getField(x.getSinkRowPosition());
                    keys.add(util.Object2String(field));
                }));
                sinkRows.put(keys, newRow);
            }
            preparedStatementQuery.close();
            return sinkRows;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void statisticalResults(Connection conn) throws Exception {
        try {
            if (this.currentTaskId == 0) {
                int del = this.jdbcDialect.deleteDataZipper(conn, jdbcSinkConfig, this.columnMappers, this.startTime);
                this.deleteCount = (long) del;
                conn.commit();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LocalDateTime endTime = LocalDateTime.now();
        StatisticalLog statisticalLog = new StatisticalLog();
        statisticalLog.setFlinkJobId(this.jobContext.getJobId());
        statisticalLog.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
        if (this.jdbcSinkConfig.getDbSchema() != null) {
            statisticalLog.setDbSchema(this.jdbcSinkConfig.getDbSchema());
        }
        statisticalLog.setTableName(this.jdbcSinkConfig.getTable());
        statisticalLog.setWriteCount(writeCount.longValue());
        statisticalLog.setModifyCount(updateCount.longValue());
        statisticalLog.setDeleteCount(deleteCount.longValue());
        statisticalLog.setInsertCount(insertCount.longValue());
        statisticalLog.setKeepCount(keepCount.longValue());
        statisticalLog.setErrorCount(errorCount.longValue());
        statisticalLog.setStartTime(startTime);
        statisticalLog.setEndTime(endTime);
        util.insertLog(statisticalLog);
    }

    @Override
    public void close() {
        try {
            this.consumeData();
            statisticalResults(conn);
            this.conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
