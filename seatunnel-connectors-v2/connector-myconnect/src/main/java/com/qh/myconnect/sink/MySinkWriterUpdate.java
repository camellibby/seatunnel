package com.qh.myconnect.sink;

import com.alibaba.fastjson2.JSONWriter;
import com.qh.myconnect.converter.CodeConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import com.alibaba.fastjson2.JSON;
import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.config.SeaTunnelJobsHistoryErrorRecord;
import com.qh.myconnect.config.StatisticalLog;
import com.qh.myconnect.config.SubTaskStatus;
import com.qh.myconnect.config.TruncateTable;
import com.qh.myconnect.config.Util;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MySinkWriterUpdate extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private SeaTunnelRowType sinkTableRowType;
    private List<SeaTunnelRow> cld = new ArrayList<>();
    private Long writeCount = 0L;
    private Long keepCount = 0L;
    private Long updateCount = 0L;
    private Long deleteCount = 0L;
    private Long insertCount = 0L;

    private Long errorCount = 0L;

    private Long qualityCount = 0L;

    private final JdbcSinkConfig jdbcSinkConfig;
    private JobContext jobContext;
    private LocalDateTime startTime;
    private int batchSize = 1000;
    private JdbcDialect jdbcDialect;
    private String table;
    private String tmpTable;

    private Map<String, String> metaDataHash;

    private Connection conn;

    private List<ColumnMapper> columnMappers = new ArrayList<>();

    private Util util = new Util();

    private final Integer currentTaskId;

    private Set sqlErrorType = new HashSet();

    private int tstampIndex = -1;

    private CodeConverter converter = new CodeConverter();

    public MySinkWriterUpdate(
            SeaTunnelRowType seaTunnelRowType,
            Context context,
            ReadonlyConfig config,
            JobContext jobContext,
            LocalDateTime startTime)
            throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.startTime = startTime;
        this.table = this.jdbcSinkConfig.getTable();
        this.currentTaskId = context.getIndexOfSubtask();
        this.tmpTable = "XJ$_" + this.jdbcSinkConfig.getTable();
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("CLICKHOUSE")) {
            this.batchSize = 3000;
        }
        this.conn = util.getConnection(this.jdbcSinkConfig);
        conn.setAutoCommit(false);
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        List<String> collect =
                Arrays.stream(this.sinkTableRowType.getFieldNames())
                        .map(String::toUpperCase)
                        .collect(Collectors.toList());
        this.tstampIndex = collect.indexOf("TSTAMP");
        this.initColumnMappers(
                this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);
        String sqlQuery =
                jdbcDialect.getSinkQueryUpdate(this.columnMappers, 0, this.jdbcSinkConfig);
        PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
        ResultSet resultSet = preparedStatementQuery.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> metaDataHash = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            metaDataHash.put(metaData.getColumnName(i + 1), metaData.getColumnTypeName(i + 1));
        }
        this.metaDataHash = metaDataHash;
        preparedStatementQuery.close();
        try {
            SubTaskStatus subTaskStatus = new SubTaskStatus();
            subTaskStatus.setFlinkJobId(this.jobContext.getJobId());
            subTaskStatus.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
            subTaskStatus.setDbSchema(this.jdbcSinkConfig.getDbSchema());
            subTaskStatus.setTableName(this.jdbcSinkConfig.getTable());
            subTaskStatus.setSubtaskIndexId(this.currentTaskId + "");
            subTaskStatus.setStatus("start");
            util.setSubTaskStatus(subTaskStatus);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void consumeData() {
        HashMap<List<String>, SeaTunnelRow> sourceRows = new HashMap<>();
        List<SeaTunnelRow> oldRows = new ArrayList<>();
        List<ColumnMapper> ucColumns =
                this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        for (SeaTunnelRow seaTunnelRow : this.cld) {
            if (seaTunnelRow != null) {
                oldRows.add(seaTunnelRow);
                SeaTunnelRow newRow = new SeaTunnelRow(columnMappers.size());
                for (int i = 0; i < this.columnMappers.size(); i++) {
                    Object field =
                            seaTunnelRow.getField(this.columnMappers.get(i).getSourceRowPosition());
                    newRow.setField(i, util.Object2String(field));
                }
                List<String> keys = new ArrayList<>();
                ucColumns.forEach(
                        x -> {
                            Object field = seaTunnelRow.getField(x.getSourceRowPosition());
                            keys.add(util.Object2String(field));
                        });
                sourceRows.put(keys, newRow);
            }
        }
        try {
            insertTmpUks(oldRows, this.metaDataHash, conn);
            this.compareData(sourceRows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void compareData(HashMap<List<String>, SeaTunnelRow> sourceRows) throws SQLException {
        HashMap<List<String>, SeaTunnelRow> sinkRows = getSinkRows(conn, sourceRows);
        List<SeaTunnelRow> needInsertRows = new ArrayList<>();
        HashMap<List<String>, SeaTunnelRow> needUpdate = new HashMap<>();
        sourceRows.forEach(
                (k, sourceRow) -> {
                    SeaTunnelRow sinkRow = sinkRows.get(k);
                    if (null != sinkRow) {
                        if (this.jdbcSinkConfig.getPreConfig().getIgnoreTstamp()) {
                            SeaTunnelRow sourceRowIgnoreTstamp = this.copyIgnoreTstamp(sourceRow);
                            SeaTunnelRow sinkRowIgnoreTstamp = this.copyIgnoreTstamp(sinkRow);
                            if (!sourceRowIgnoreTstamp.equals(sinkRowIgnoreTstamp)) {
                                needUpdate.put(k, sourceRow);
                            }
                            else {
                                this.keepCount++;
                            }
                        }
                        else {
                            if (!sourceRow.equals(sinkRow)) {
                                needUpdate.put(k, sourceRow);
                            }
                            else {
                                this.keepCount++;
                            }
                        }
                    }
                    else {
                        needInsertRows.add(sourceRow);
                    }
                });
        doInsert(needInsertRows, this.metaDataHash, conn);
        doUpdate(needUpdate, this.metaDataHash);
    }

    private boolean containsAtLeastTwoDotsRegex(String str) {
        Pattern pattern = Pattern.compile("\\..*\\.");
        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }

    private void initColumnMappers(
            JdbcSinkConfig jdbcSinkConfig,
            SeaTunnelRowType sourceRowType,
            SeaTunnelRowType sinkTableRowType,
            Connection conn)
            throws SQLException {
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        Map<String, String> codeMapper = jdbcSinkConfig.getCodeMapper();
        if (codeMapper != null) {
            Optional<String> any = codeMapper.values().stream().filter(x -> x.startsWith("ENCRYPT.") && containsAtLeastTwoDotsRegex(x)).findAny();
            any.ifPresent(x -> {
                converter = new CodeConverter(x.split("\\.")[2]);
            });
        }
        Map<String, String> dmMap = new HashMap<>();
        List<String> allDms = new ArrayList<>();
        if (codeMapper != null) {
            allDms = codeMapper.values().stream().filter(x -> x.startsWith("DM")).distinct().collect(Collectors.toList());
        }
        for (String allDm : allDms) {
            String[] split = allDm.split("\\.");
            String sql = String.format("select %s,%s from %s", split[2], split[3], split[1]);
            try (Connection con = util.getPanguConnection();
                 Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    dmMap.put(allDm + "." + rs.getString(split[2]), rs.getString(split[3]));
                }
            }
        }
        converter.setDmMap(dmMap);
        fieldMapper.forEach(
                (k, v) -> {
                    ColumnMapper columnMapper = new ColumnMapper();
                    columnMapper.setSourceColumnName(k);
                    columnMapper.setSourceRowPosition(sourceRowType.indexOf(k));
                    String typeNameSS =
                            sourceRowType
                                    .getFieldType(sourceRowType.indexOf(k))
                                    .getTypeClass()
                                    .getName();
                    columnMapper.setSourceColumnTypeName(typeNameSS);
                    columnMapper.setSinkColumnName(v);
                    columnMapper.setSinkRowPosition(sinkTableRowType.indexOf(v));
                    String typeNameSK =
                            sinkTableRowType
                                    .getFieldType(sinkTableRowType.indexOf(v))
                                    .getTypeClass()
                                    .getName();
                    columnMapper.setSinkColumnTypeName(typeNameSK);

                    for (String primaryKey : jdbcSinkConfig.getPrimaryKeys()) {
                        if (primaryKey.equalsIgnoreCase(v)) {
                            columnMapper.setUc(true);
                        }
                    }
                    try {
                        ResultSetMetaData metaData =
                                this.jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
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
                    if (codeMapper != null) {
                        String safeCode = codeMapper.get(v);
                        if (safeCode != null && StringUtils.isNoneBlank(safeCode)) {
                            if (safeCode.startsWith("DM")) {
                                columnMapper.setConverter(converter.dmConverter(safeCode));
                            }
                            else if (safeCode.startsWith("ENCRYPT")) {
                                columnMapper.setConverter(converter.encryptConverter(safeCode));
                            }
                        }
                    }
                    columnMappers.add(columnMapper);
                });
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (element.getRowKind().equals(RowKind.INSERT)) {
            this.writeCount++;
            if(this.jdbcSinkConfig.isOpenQuality()){
//                this.qualityCount++;
//                return;
            }
            for (int i = 0; i < columnMappers.size(); i++) {
                Integer sourceRowPosition = columnMappers.get(i).getSourceRowPosition();
                if (sourceRowPosition != null) {
                    element.setField(sourceRowPosition, columnMappers.get(i).getConverter().apply(element.getField(sourceRowPosition)));
                }
            }
            this.cld.add(element);
            if (this.writeCount % batchSize == 0) {
                this.consumeData();
                cld.clear();
            }
        }
        else if (jobContext.getJobMode().equals(JobMode.STREAMING)
                 && element.getRowKind().equals(RowKind.DELETE)) {
            this.consumeData();
            cld.clear();
            {
                try {
                    List<ColumnMapper> ucColumns =
                            this.columnMappers.stream()
                                    .filter(ColumnMapper::isUc)
                                    .collect(Collectors.toList());
                    long del = 0;
                    if (this.jdbcSinkConfig.getDbSchema() != null
                        && !this.jdbcSinkConfig.getDbSchema().equals("")) {
                        del =
                                this.jdbcDialect.deleteData(
                                        conn,
                                        this.jdbcSinkConfig.getDbSchema() + "." + table,
                                        this.jdbcSinkConfig.getDbSchema() + "." + tmpTable,
                                        ucColumns);
                    }
                    else if (null != this.jdbcSinkConfig.getPreConfig().getClusterName()
                             && !this.jdbcSinkConfig
                            .getPreConfig()
                            .getClusterName()
                            .equalsIgnoreCase("")) {
                        del =
                                this.jdbcDialect.deleteDataOnCluster(
                                        conn,
                                        table,
                                        tmpTable,
                                        ucColumns,
                                        this.jdbcSinkConfig.getPreConfig().getClusterName());
                    }
                    else {
                        del = this.jdbcDialect.deleteData(conn, table, tmpTable, ucColumns);
                    }
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            {
                TruncateTable truncateTable = new TruncateTable();
                truncateTable.setFlinkJobId(this.jobContext.getJobId());
                truncateTable.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
                if (this.jdbcSinkConfig.getDbSchema() != null
                    && !this.jdbcSinkConfig.getDbSchema().equalsIgnoreCase("")) {
                    truncateTable.setDbSchema(this.jdbcSinkConfig.getDbSchema());
                    truncateTable.setTableName(this.jdbcSinkConfig.getDbSchema() + "." + tmpTable);
                }
                else {
                    if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                        truncateTable.setTableName(String.format("`%s`", tmpTable));
                    }
                    else {
                        truncateTable.setTableName(tmpTable);
                    }
                }
                util.truncateTable(truncateTable);
            }
        }
    }

    private void doInsert(
            List<SeaTunnelRow> rows, Map<String, String> metaDataHash, Connection connection)
            throws SQLException {
        Long tmpInsertCount = null;
        try {
            List<String> newColumns = new ArrayList<>();
            List<String> newValues = new ArrayList<>();
            for (ColumnMapper column : columnMappers) {
                newColumns.add(column.getSinkColumnName());
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
                    jdbcDialect.setPreparedStatementValueByDbType(
                            i + 1,
                            preparedStatement,
                            dbType,
                            (String) row.getField(i)
                    );
                }
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
            insertToDbOneByOne(rows);
        }
    }

    private void insertToDbOneByOne(List<SeaTunnelRow> rows) {
        try {
            List<String> columns =
                    this.columnMappers.stream()
                            .map(x -> x.getSinkColumnName())
                            .collect(Collectors.toList());
            List<String> values =
                    this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = jdbcDialect.insertTableSql(this.jdbcSinkConfig, columns, values);
            for (SeaTunnelRow seaTunnelRow : rows) {
                if (seaTunnelRow != null) {
                    PreparedStatement psUpsert = conn.prepareStatement(sql);
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Object field = seaTunnelRow.getField(i);
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(
                                i + 1,
                                psUpsert,
                                dbType,
                                util.Object2String(field)
                        );
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
                        if (this.jobContext.getIsRecordErrorData() == 1
                            && this.errorCount <= this.jobContext.getMaxRecordNumber()
                            && !sqlErrorType.contains(ee.getMessage())) {
                            LinkedHashMap<String, Object> jsonObject = new LinkedHashMap<>();
                            for (int i = 0; i < this.columnMappers.size(); i++) {
                                jsonObject.put(
                                        this.columnMappers.get(i).getSourceColumnName(),
                                        seaTunnelRow.getField(i));
                            }
                            log.info(
                                    JSON.toJSONString(
                                            jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
                            SeaTunnelJobsHistoryErrorRecord errorRecord =
                                    new SeaTunnelJobsHistoryErrorRecord();
                            errorRecord.setFlinkJobId(this.jobContext.getJobId());
                            errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                            errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                            errorRecord.setTableName(jdbcSinkConfig.getTable());
                            errorRecord.setErrorData(
                                    JSON.toJSONString(
                                            jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
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

    private void doUpdate(
            HashMap<List<String>, SeaTunnelRow> rows, Map<String, String> metaDataHash)
            throws SQLException {
        Long tmpUpdateCount = null;
        try {
            List<ColumnMapper> listUc =
                    this.columnMappers.stream()
                            .filter(ColumnMapper::isUc)
                            .collect(Collectors.toList());
            List<ColumnMapper> columnMappers =
                    this.columnMappers.stream().filter(x -> !x.isUc()).collect(Collectors.toList());
            String templateInsert =
                    " #set($separator = '') "
                    + "#set($separator2 = '') "
                    + "update ${table} set "
                    + "#foreach( $item in $columns )"
                    + " $separator  $item = ?"
                    + "#set($separator = ',') "
                    + "#end"
                    + " where "
                    + "#foreach( $item in $pks )"
                    + " $separator2  $item = ?"
                    + "#set($separator2 = ' and  ') "
                    + "#end";
            VelocityEngine ve = new VelocityEngine();
            ve.init();
            Velocity.init();
            VelocityContext context = new VelocityContext();
            if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
                context.put("table", jdbcSinkConfig.getDbSchema() + "." + jdbcSinkConfig.getTable());
            }
            else {
                context.put("table", jdbcSinkConfig.getTable());
            }
            context.put("columns", columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
            context.put("pks", listUc.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
            StringWriter writer = new StringWriter();
            Velocity.evaluate(context, writer, "mystring2", templateInsert);
            String updateSql = writer.toString();
            if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                updateSql = updateSql + " SETTINGS mutations_sync=0";
            }
            PreparedStatement preparedStatement = conn.prepareStatement(updateSql);
            tmpUpdateCount = this.updateCount;
            boolean hasError = false;
            for (SeaTunnelRow row : rows.values()) {
                for (int i = 0; i < columnMappers.size(); i++) {
                    String column = columnMappers.get(i).getSinkColumnName();
                    String dbType = metaDataHash.get(column);
                    this.jdbcDialect.setPreparedStatementValueByDbType(
                            i + 1,
                            preparedStatement,
                            dbType,
                            (String) row.getField(columnMappers.get(i).getSinkRowPosition())
                    );
                }
                for (int i = 0; i < listUc.size(); i++) {
                    String column = listUc.get(i).getSinkColumnName();
                    String dbType = metaDataHash.get(column);
                    this.jdbcDialect.setPreparedStatementValueByDbType(
                            i + 1 + columnMappers.size(),
                            preparedStatement,
                            dbType,
                            (String) row.getField(listUc.get(i).getSinkRowPosition())
                    );
                }
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
            try {
                conn.rollback();
                this.updateCount = tmpUpdateCount;
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            updateDbOneByOne(rows);
        }
    }

    private void updateDbOneByOne(HashMap<List<String>, SeaTunnelRow> rows) {
        try {
            List<ColumnMapper> listUc =
                    this.columnMappers.stream()
                            .filter(ColumnMapper::isUc)
                            .collect(Collectors.toList());
            List<ColumnMapper> columnMappers =
                    this.columnMappers.stream().filter(x -> !x.isUc()).collect(Collectors.toList());
            String templateInsert =
                    "#set($separator = '') "
                    + "#set($separator2 = '') "
                    + "update `${table}` set "
                    + "#foreach( $item in $columns )"
                    + " $separator  $item = ?"
                    + "#set($separator = ', ') "
                    + "#end"
                    + " where "
                    + "#foreach( $item in $pks )"
                    + " $separator2  $item = ?"
                    + "#set($separator2 = ' and  ') "
                    + "#end";
            VelocityEngine ve = new VelocityEngine();
            ve.init();
            Velocity.init();
            VelocityContext context = new VelocityContext();
            if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
                context.put("table", jdbcSinkConfig.getDbSchema() + "." + jdbcSinkConfig.getTable());
            }
            else {
                context.put("table", jdbcSinkConfig.getTable());
            }
            context.put("columns", columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
            context.put("pks", listUc.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
            StringWriter writer = new StringWriter();
            Velocity.evaluate(context, writer, "mystring1", templateInsert);
            String updateSql = writer.toString();
            if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                updateSql = updateSql + " SETTINGS mutations_sync=0";
            }
            for (SeaTunnelRow row : rows.values()) {
                PreparedStatement preparedStatement = conn.prepareStatement(updateSql);
                for (int i = 0; i < columnMappers.size(); i++) {
                    String column = columnMappers.get(i).getSinkColumnName();
                    String dbType = metaDataHash.get(column);
                    this.jdbcDialect.setPreparedStatementValueByDbType(
                            i + 1,
                            preparedStatement,
                            dbType,
                            (String) row.getField(columnMappers.get(i).getSinkRowPosition())
                    );
                }
                for (int i = 0; i < listUc.size(); i++) {
                    String column = listUc.get(i).getSinkColumnName();
                    String dbType = metaDataHash.get(column);
                    this.jdbcDialect.setPreparedStatementValueByDbType(
                            i + 1 + columnMappers.size(),
                            preparedStatement,
                            dbType,
                            (String) row.getField(listUc.get(i).getSinkRowPosition())
                    );
                }
                try {
                    preparedStatement.addBatch();
                    preparedStatement.executeBatch();
                    conn.commit();
                    this.updateCount++;
                    preparedStatement.clearBatch();
                    preparedStatement.close();
                } catch (SQLException ee) {
                    this.errorCount++;
                    if (this.jobContext.getIsRecordErrorData() == 1
                        && this.errorCount.longValue() <= this.jobContext.getMaxRecordNumber()
                        && !sqlErrorType.contains(ee.getMessage())) {
                        LinkedHashMap<String, Object> jsonObject = new LinkedHashMap<>();
                        for (int i = 0; i < this.columnMappers.size(); i++) {
                            jsonObject.put(
                                    this.columnMappers.get(i).getSourceColumnName(),
                                    row.getField(i));
                        }
                        SeaTunnelJobsHistoryErrorRecord errorRecord =
                                new SeaTunnelJobsHistoryErrorRecord();
                        errorRecord.setFlinkJobId(this.jobContext.getJobId());
                        errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                        errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                        errorRecord.setTableName(jdbcSinkConfig.getTable());
                        errorRecord.setErrorData(JSON.toJSONString(jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private HashMap<List<String>, SeaTunnelRow> getSinkRows(
            Connection conn, HashMap<List<String>, SeaTunnelRow> sourceRows) {
        AtomicInteger tmp = new AtomicInteger();
        HashMap<List<String>, SeaTunnelRow> sinkRows = new HashMap<>();
        String sqlQuery =
                jdbcDialect.getSinkQueryUpdate(
                        this.columnMappers, sourceRows.size(), this.jdbcSinkConfig);
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        try {
            PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
            sourceRows.forEach(
                    (k, v) -> {
                        for (int i = 0; i < ucColumns.size(); i++) {
                            try {
                                jdbcDialect.setPreparedStatementValue(
                                        preparedStatementQuery,
                                        (i + 1) + tmp.get() * ucColumns.size(),
                                        k.get(i));
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
                ucColumns.forEach(
                        (x -> {
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
            SubTaskStatus subTaskStatus = new SubTaskStatus();
            subTaskStatus.setFlinkJobId(this.jobContext.getJobId());
            subTaskStatus.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
            subTaskStatus.setDbSchema(this.jdbcSinkConfig.getDbSchema());
            subTaskStatus.setTableName(this.jdbcSinkConfig.getTable());
            subTaskStatus.setSubtaskIndexId(this.currentTaskId + "");
            subTaskStatus.setStatus("done");
            util.setSubTaskStatus(subTaskStatus);
            List<SubTaskStatus> subTaskStatus1 = util.getSubTaskStatus(subTaskStatus);
            int done =
                    (int)
                            subTaskStatus1.stream()
                                    .filter(x -> !x.getStatus().equalsIgnoreCase("done"))
                                    .count();
            if (done == 0) {
                List<ColumnMapper> ucColumns =
                        this.columnMappers.stream()
                                .filter(ColumnMapper::isUc)
                                .collect(Collectors.toList());
                long del = 0;
                if (this.jdbcSinkConfig.getDbSchema() != null
                    && !this.jdbcSinkConfig.getDbSchema().equals("")) {
                    del =
                            this.jdbcDialect.deleteData(
                                    conn,
                                    this.jdbcSinkConfig.getDbSchema() + "." + table,
                                    this.jdbcSinkConfig.getDbSchema() + "." + tmpTable,
                                    ucColumns);
                }
                else if (null != this.jdbcSinkConfig.getPreConfig().getClusterName()
                         && !this.jdbcSinkConfig
                        .getPreConfig()
                        .getClusterName()
                        .equalsIgnoreCase("")) {
                    del =
                            this.jdbcDialect.deleteDataOnCluster(
                                    conn,
                                    table,
                                    tmpTable,
                                    ucColumns,
                                    this.jdbcSinkConfig.getPreConfig().getClusterName());
                }
                else {
                    del = this.jdbcDialect.deleteData(conn, table, tmpTable, ucColumns);
                }
                conn.commit();
                deleteCount = del;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StatisticalLog statisticalLog = new StatisticalLog();
        LocalDateTime endTime = LocalDateTime.now();
        statisticalLog.setFlinkJobId(this.jobContext.getJobId());
        statisticalLog.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
        if (this.jdbcSinkConfig.getDbSchema() != null) {
            statisticalLog.setDbSchema(this.jdbcSinkConfig.getDbSchema());
        }
        statisticalLog.setTableName(this.jdbcSinkConfig.getTable());
        statisticalLog.setWriteCount(writeCount);
        statisticalLog.setQualityCount(qualityCount);
        statisticalLog.setModifyCount(updateCount);
        statisticalLog.setDeleteCount(deleteCount);
        statisticalLog.setInsertCount(insertCount);
        statisticalLog.setKeepCount(keepCount);
        statisticalLog.setErrorCount(errorCount);
        statisticalLog.setStartTime(startTime);
        statisticalLog.setEndTime(endTime);
        util.insertLog(statisticalLog);
    }

    private void insertTmpUks(
            List<SeaTunnelRow> oldRows, Map<String, String> metaDataHash, Connection conn)
            throws SQLException {
        List<ColumnMapper> ucColumns =
                this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        JdbcSinkConfig newJdbcSinkConfig =
                JdbcSinkConfig.builder()
                        .dbSchema(jdbcSinkConfig.getDbSchema())
                        .table(tmpTable)
                        .build();
        String insert =
                jdbcDialect.insertTableSql(
                        newJdbcSinkConfig,
                        ucColumns.stream()
                                .map(x -> x.getSinkColumnName())
                                .collect(Collectors.toList()),
                        ucColumns.stream().map(x -> "?").collect(Collectors.toList()));
        PreparedStatement preparedStatement = conn.prepareStatement(insert);
        for (SeaTunnelRow oldRow : oldRows) {
            for (int i = 0; i < ucColumns.size(); i++) {
                String dbType = metaDataHash.get(ucColumns.get(i).getSinkColumnName());
                jdbcDialect.setPreparedStatementValueByDbType(
                        i + 1,
                        preparedStatement,
                        dbType,
                        util.Object2String(
                                oldRow.getField(ucColumns.get(i).getSourceRowPosition())));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
        conn.commit();
    }

    @Override
    public void close() {
        try {
            this.consumeData();
            statisticalResults(conn);
//            jdbcSinkConfig.getPreConfig().dropUcTable(conn, jdbcSinkConfig);
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SeaTunnelRow copyIgnoreTstamp(SeaTunnelRow oldRow) {
        Object[] newFields = new Object[oldRow.getArity()];
        Object[] fields = new Object[oldRow.getArity()];
        System.arraycopy(oldRow.getFields(), 0, fields, 0, newFields.length);
        if (tstampIndex != -1) {
            fields[tstampIndex] = null;
        }
        System.arraycopy(fields, 0, newFields, 0, newFields.length);
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(oldRow.getRowKind());
        newRow.setTableId(oldRow.getTableId());
        return newRow;
    }
}
