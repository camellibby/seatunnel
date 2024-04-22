package com.qh.myconnect.sink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.qh.myconnect.config.*;
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
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class MySinkWriterComplete extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private final Context context;
    private List<SeaTunnelRow> cld = new ArrayList<>();
    private Long writeCount = 0L;

    private Long insertCount = 0L;

    private Long deleteCount = 0l;

    private Long errorCount = 0L;
    private final JdbcSinkConfig jdbcSinkConfig;
    private JobContext jobContext;

    private JdbcDialect jdbcDialect;

    private LocalDateTime startTime;
    private Map<String, String> metaDataHash;

    private Connection conn;

    private String table;
    private List<ColumnMapper> columnMappers = new ArrayList<>();

    private SeaTunnelRowType sinkTableRowType;

    private final Util util = new Util();
    private int batchSize = 2000;

    private Long tableCount;

    private PreConfig preConfig;

    private final Integer currentTaskId;

    private Set sqlErrorType = new HashSet();

    public MySinkWriterComplete(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, Long tableCount) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.context = context;
        this.currentTaskId = context.getIndexOfSubtask();
        log.info("currentTaskId:" + this.currentTaskId);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.table = this.jdbcSinkConfig.getTable();
        this.preConfig = jdbcSinkConfig.getPreConfig();
        this.startTime = LocalDateTime.now();
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
        this.conn.setAutoCommit(false);
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);
        this.tableCount = tableCount;
        if (this.preConfig.isCleanTableWhenComplete() && this.preConfig.isCleanTableWhenCompleteNoDataIn()) {
            this.deleteCount = this.tableCount;
        }
        String sqlQuery = jdbcDialect.getSinkQueryUpdate(this.columnMappers, 0, jdbcSinkConfig);
        PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
        ResultSet resultSet = preparedStatementQuery.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> metaDataHash = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            metaDataHash.put(metaData.getColumnName(i + 1), metaData.getColumnTypeName(i + 1));
        }
        this.metaDataHash = metaDataHash;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        this.writeCount++;
        if (this.writeCount == 1 && this.preConfig.isCleanTableWhenComplete()) {
            TruncateTable truncateTable = new TruncateTable();
            truncateTable.setFlinkJobId(this.jobContext.getJobId());
            truncateTable.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
            if (this.jdbcSinkConfig.getDbSchema() != null && !this.jdbcSinkConfig.getDbSchema().equalsIgnoreCase("")) {
                truncateTable.setDbSchema(this.jdbcSinkConfig.getDbSchema());
                truncateTable.setTableName(this.jdbcSinkConfig.getDbSchema() + "." + this.jdbcSinkConfig.getTable());
            } else {
                if(this.jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")){
                    truncateTable.setTableName(String.format("`%s`", this.jdbcSinkConfig.getTable()));
                }else{
                    truncateTable.setTableName(this.jdbcSinkConfig.getTable());
                }
            }
            util.truncateTable(truncateTable);
            this.deleteCount = this.tableCount;
        }
        this.cld.add(element);
        if (this.writeCount.longValue() % batchSize == 0) {
            this.insertToDb();
            cld.clear();
        }
    }

    @Override
    public void close() {
        try {
            this.insertToDb();
            conn.close();
            statisticalResults();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void insertToDb() {
        Long tmpInsertCount = null;
        try {
            List<String> columns = this.columnMappers.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = jdbcDialect.insertTableSql(this.jdbcSinkConfig, columns, values);
            PreparedStatement psUpsert = conn.prepareStatement(sql);
            tmpInsertCount = this.insertCount;
            boolean hasError = false;
            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = seaTunnelRow.getField(valueIndex);
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    this.insertCount++;
                    try {
                        psUpsert.addBatch();
                    } catch (SQLException e) {
                        hasError = true;
                        break;
                    }

                }
            }
            if (hasError) {
                throw new RuntimeException();
            }
            psUpsert.executeBatch();
            conn.commit();
            psUpsert.clearBatch();
            psUpsert.close();
        } catch (Exception e) {
            try {
                conn.rollback();
                this.insertCount = tmpInsertCount;
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            insertToDbOneByOne();
        }
    }

    public void statisticalResults() throws Exception {
        LocalDateTime endTime = LocalDateTime.now();
        StatisticalLog statisticalLog = new StatisticalLog();
        statisticalLog.setFlinkJobId(this.jobContext.getJobId());
        statisticalLog.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
        if (this.jdbcSinkConfig.getDbSchema() != null) {
            statisticalLog.setDbSchema(this.jdbcSinkConfig.getDbSchema());
        }
        statisticalLog.setTableName(this.jdbcSinkConfig.getTable());
        statisticalLog.setWriteCount(writeCount);
        statisticalLog.setModifyCount(0L);
        statisticalLog.setDeleteCount(deleteCount);
        statisticalLog.setInsertCount(this.insertCount);
        statisticalLog.setKeepCount(0L);
        statisticalLog.setErrorCount(errorCount);
        statisticalLog.setStartTime(startTime);
        statisticalLog.setEndTime(endTime);
        util.insertLog(statisticalLog);
    }


    private void insertToDbOneByOne() {
        try {
            List<String> columns = this.columnMappers.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = jdbcDialect.insertTableSql(this.jdbcSinkConfig, columns, values);

            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    PreparedStatement psUpsert = conn.prepareStatement(sql);
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = seaTunnelRow.getField(valueIndex);
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    try {
                        psUpsert.addBatch();
                        psUpsert.executeBatch();
                        conn.commit();
                        psUpsert.clearBatch();
                        psUpsert.close();
                        this.insertCount++;
                    } catch (SQLException ee) {
                        this.errorCount++;
                        if (this.jobContext.getIsRecordErrorData() == 1 && this.errorCount <= this.jobContext.getMaxRecordNumber() && !sqlErrorType.contains(ee.getMessage())) {
                            LinkedHashMap<String, Object> jsonObject = new LinkedHashMap<>();
                            for (int i = 0; i < this.columnMappers.size(); i++) {
                                jsonObject.put(this.columnMappers.get(i).getSourceColumnName(), seaTunnelRow.getField(i));
                            }
                            log.info(JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue));
                            SeaTunnelJobsHistoryErrorRecord errorRecord = new SeaTunnelJobsHistoryErrorRecord();
                            errorRecord.setFlinkJobId(this.jobContext.getJobId());
                            errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                            errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                            errorRecord.setTableName(jdbcSinkConfig.getTable());
                            errorRecord.setErrorData(JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue));
                            errorRecord.setErrorMessage(ee.getMessage());
                            sqlErrorType.add(ee.getMessage());
                            try {
                                util.insertErrorData(errorRecord);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void initColumnMappers(JdbcSinkConfig jdbcSinkConfig, SeaTunnelRowType sourceRowType, SeaTunnelRowType sinkTableRowType, Connection conn) throws SQLException {
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        fieldMapper.forEach((k, v) -> {
            ColumnMapper columnMapper = new ColumnMapper();
            columnMapper.setSourceColumnName(k);
            columnMapper.setSourceRowPosition(sourceRowType.indexOf(k));
            String typeNames = sourceRowType.getFieldType(sourceRowType.indexOf(k)).getTypeClass().getName();
            columnMapper.setSourceColumnTypeName(typeNames);
            columnMapper.setSinkColumnName(v);
            columnMapper.setSinkRowPosition(sinkTableRowType.indexOf(v));
            String typeNameSK = sinkTableRowType.getFieldType(sinkTableRowType.indexOf(v)).getTypeClass().getName();
            columnMapper.setSinkColumnTypeName(typeNameSK);
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


}
