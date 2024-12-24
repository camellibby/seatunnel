package com.qh.myconnect.sink;

import com.alibaba.fastjson2.JSONWriter;
import com.qh.myconnect.config.SubTaskStatus;
import com.qh.myconnect.converter.CodeConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import com.alibaba.fastjson2.JSON;
import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.config.PreConfig;
import com.qh.myconnect.config.SeaTunnelJobsHistoryErrorRecord;
import com.qh.myconnect.config.StatisticalLog;
import com.qh.myconnect.config.Util;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MySinkWriterZipper extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private final Context context;
    private List<SeaTunnelRow> cld = new ArrayList<>();
    private Long writeCount = 0L;

    private Long insertCount = 0L;
    private Long keepCount = 0L;

    private Long updateCount = 0L;
    private Long deleteCount = 0L;

    private Long errorCount = 0L;

    private Long qualityCount = 0L;
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
    private int batchSize = 1000;
    private PreConfig preConfig;

    private final Integer currentTaskId;

    private Set sqlErrorType = new HashSet();
    private String tmpTable;
    private CodeConverter converter = new CodeConverter();
    private Set<String> ignoreColumns = new HashSet<>();

    public MySinkWriterZipper(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, LocalDateTime startTime) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.context = context;
        this.currentTaskId = context.getIndexOfSubtask();
        log.info("currentTaskId:" + this.currentTaskId);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.tmpTable = "XJ$_" + this.jdbcSinkConfig.getTable();
        this.table = this.jdbcSinkConfig.getTable();
        this.preConfig = jdbcSinkConfig.getPreConfig();
        if (preConfig != null && preConfig.getIgnoreTstamp()) {
            ignoreColumns.add("TSTAMP");
        }
        if (preConfig != null && preConfig.getIgnoreColumns() != null) {
            ignoreColumns.addAll(preConfig.getIgnoreColumns());
        }
        this.startTime = startTime;
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
        this.conn.setAutoCommit(false);
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);
        String sqlQuery = jdbcDialect.getSinkQueryUpdate(this.columnMappers, 0, jdbcSinkConfig);
        PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
        ResultSet resultSet = preparedStatementQuery.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> metaDataHash = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            metaDataHash.put(metaData.getColumnName(i + 1), metaData.getColumnTypeName(i + 1));
        }
        this.metaDataHash = metaDataHash;
        conn.commit();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        this.writeCount++;
        if (this.jdbcSinkConfig.isOpenQuality()) {
//            this.qualityCount++;
//            return;
        }
        this.cld.add(element);
        if (this.writeCount.longValue() % batchSize == 0 || this.jobContext.getJobMode().equals(JobMode.STREAMING)) {
            this.insertToDb();
            cld.clear();
        }
    }

    @Override
    public void close() {
        try {
            this.insertToDb();
            statisticalResults(conn);
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void insertToDb() {
        String sql = null;
        try {
            List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            sql = jdbcDialect.insertTmpTableSql(this.jdbcSinkConfig, columns, values);
            PreparedStatement psUpsert = conn.prepareStatement(sql);
            boolean hasError = false;
            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = this.columnMappers.get(i).getConverter().apply(seaTunnelRow.getField(valueIndex));
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
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
            log.error("错误sql:" + sql, e);
            try {
                conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            insertToDbOneByOne();
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
            int done = (int) subTaskStatus1.stream().filter(x -> !x.getStatus().equalsIgnoreCase("done")).count();
            log.info("subTaskStatus:" + done);
            if (done == 0) {
                List<ColumnMapper> ucColumns =
                        this.columnMappers.stream()
                                .filter(ColumnMapper::isUc)
                                .collect(Collectors.toList());
                //处理删除数据
                {
                    long del = 0;
                    if (this.jdbcSinkConfig.getDbSchema() != null
                        && !this.jdbcSinkConfig.getDbSchema().isEmpty()) {
                        del =
                                this.jdbcDialect.deleteDataZipper(
                                        jdbcSinkConfig,
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
                                this.jdbcDialect.deleteDataZipperCluster(
                                        jdbcSinkConfig,
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
                compareTables(conn, this.table);
                //处理新增数据
                {
                    List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
                    List<String> ucs =
                            columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
                    String insertSqlCount =
                            this.jdbcDialect.insertDataCountZipper(jdbcSinkConfig, tmpTable, ucs);
                    ResultSet resultSet = conn.createStatement().executeQuery(insertSqlCount);
                    resultSet.next();
                    insertCount = resultSet.getLong(1);
                    String insertSql = this.jdbcDialect.insertDataZipper(jdbcSinkConfig, tmpTable, columns, ucs);
                    conn.createStatement().execute(insertSql);
                    conn.commit();
                }
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

    private void insertToDbOneByOne() {
        try {
            List<String> columns = this.columnMappers.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = jdbcDialect.insertTmpTableSql(this.jdbcSinkConfig, columns, values);
            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    PreparedStatement psUpsert = conn.prepareStatement(sql);
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = this.columnMappers.get(i).getConverter().apply(seaTunnelRow.getField(valueIndex));
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
                    } catch (SQLException ee) {
                        this.errorCount++;
                        if (this.jobContext.getIsRecordErrorData() == 1 && this.errorCount <= this.jobContext.getMaxRecordNumber() && !sqlErrorType.contains(ee.getMessage())) {
                            LinkedHashMap<String, Object> jsonObject = new LinkedHashMap<>();
                            for (int i = 0; i < this.columnMappers.size(); i++) {
                                jsonObject.put(this.columnMappers.get(i).getSourceColumnName(), seaTunnelRow.getField(i));
                            }
                            log.info(JSON.toJSONString(jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
                            SeaTunnelJobsHistoryErrorRecord errorRecord = new SeaTunnelJobsHistoryErrorRecord();
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
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean containsAtLeastTwoDotsRegex(String str) {
        Pattern pattern = Pattern.compile("\\..*\\.");
        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }

    private void initColumnMappers(JdbcSinkConfig jdbcSinkConfig, SeaTunnelRowType sourceRowType, SeaTunnelRowType sinkTableRowType, Connection conn) throws SQLException {
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
            try (Connection con = util.getPanguConnection(); Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    dmMap.put(allDm + "." + rs.getString(split[2]), rs.getString(split[3]));
                }
            }
        }
        converter.setDmMap(dmMap);
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

    private void compareTables(Connection conn, String tableName) throws SQLException {
        int indexOfSubtask = context.getIndexOfSubtask();
        log.info("subtask:{} compare table:{}", indexOfSubtask, tableName);
        List<String> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        String querySource = this.jdbcDialect.getDataSql(this.jdbcSinkConfig, this.columnMappers, this.tmpTable);
        String queryTarget = this.jdbcDialect.getDataSqlZipper(this.jdbcSinkConfig, this.columnMappers, tableName);
        try (Statement stmt1 = conn.createStatement(); Statement stmt2 = conn.createStatement(); ResultSet rs1 = stmt1.executeQuery(querySource); ResultSet rs2 = stmt2.executeQuery(queryTarget)) {
            ResultSetMetaData md1 = rs1.getMetaData();
            int columnCount = md1.getColumnCount();
            StringBuilder sourceKey = new StringBuilder();
            StringBuilder targetKey = new StringBuilder();
            boolean rs1Done = false;
            boolean rs2Done = false;
            if (rs1.next()) {
                for (String ucColumn : ucColumns) {
                    String object = util.Object2String(rs1.getObject(ucColumn));
                    sourceKey.append(object);
                }
            }
            if (rs2.next()) {
                for (String ucColumn : ucColumns) {
                    String object = util.Object2String(rs2.getObject(ucColumn));
                    targetKey.append(object);
                }
            }

//            if (sourceKey.length() == 0) {
//                log.info("全部是删除");
//            }
//            if (targetKey.length() == 0) {
//                log.info("全收是新增");
//            }
            if (!sourceKey.toString().isEmpty() && !targetKey.toString().isEmpty()) {
                while (true) {
                    int result = sourceKey.toString().compareTo(targetKey.toString());
                    if (result == 0) {
//                        log.info("主键相等");
                        //对比数据
                        boolean change = false;
                        for (int i = 1; i <= columnCount; i++) {
                            Object value1 = rs1.getObject(i);
                            Object value2 = rs2.getObject(i);
                            if (!Objects.equals(value1, value2)) {
                                log.info("发现字段区别:" + md1.getColumnName(i));
                                log.info("新值: " + value1);
                                log.info("老值: " + value2);
                                if (ignoreColumns.contains(md1.getColumnName(i))) {
                                    log.info("该字段已加入忽略对比:" + md1.getColumnName(i));
                                }
                                else {
                                    String updateSql = jdbcDialect.updateTableSqlZipper(jdbcSinkConfig, ucColumns);
                                    PreparedStatement preparedStatement = conn.prepareStatement(updateSql);
                                    for (int j = 0; j < ucColumns.size(); j++) {
                                        preparedStatement.setObject(j + 1, rs2.getObject(ucColumns.get(j)));
                                    }
                                    preparedStatement.executeUpdate();
                                    preparedStatement.close();
                                    conn.commit();
                                    change = true;
                                }
                            }
                        }
                        if (change) {
                            updateCount++;
                        }
                        else {
                            keepCount++;
                        }
//                        log.info("同时往下移动");
                        if (rs1.next()) {
                            sourceKey.setLength(0);
                            for (String ucColumn : ucColumns) {
                                String object = util.Object2String(rs1.getObject(ucColumn));
                                sourceKey.append(object);
                            }
                        }
                        else {
                            rs1Done = true;
//                            while (rs2.next()) {
//                                log.info("剩下的rs2全部要删除");
//                            }
                            rs2Done = true;
                        }
                        if (rs2.next()) {
                            targetKey.setLength(0);
                            for (String ucColumn : ucColumns) {
                                String object = util.Object2String(rs2.getObject(ucColumn));
                                targetKey.append(object);
                            }
                        }
                        else {
                            rs2Done = true;
//                            while (rs1.next()) {
//                                log.info("剩下的rs1全部要插入");
//                            }
                            rs1Done = true;
                        }
                    }
                    if (result > 0) {
//                        log.info("rs2需要往下移,删除rs2");
                        if (rs2.next()) {
                            targetKey.setLength(0);
                            for (String ucColumn : ucColumns) {
                                String object = util.Object2String(rs2.getObject(ucColumn));
                                targetKey.append(object);
                            }
                        }
                        else {
                            rs2Done = true;
//                            while (rs1.next()) {
//                                log.info("剩下的rs1全部要插入");
//                            }
                            rs1Done = true;
                        }
                    }
                    if (result < 0) {
//                        log.info("rs1需要往下移,插入rs1");
//                        log.info("sourceKey " + sourceKey);
                        if (rs1.next()) {
                            sourceKey.setLength(0);
                            for (String ucColumn : ucColumns) {
                                String object = util.Object2String(rs1.getObject(ucColumn));
                                sourceKey.append(object);
                            }
                        }
                        else {
                            rs1Done = true;
//                            while (rs2.next()) {
//                                log.info("剩下的rs2全部要删除");
//                            }
                            rs2Done = true;
                        }
                    }
                    if (rs1Done && rs2Done) {
                        break;
                    }
                }
            }
        }

    }
}
