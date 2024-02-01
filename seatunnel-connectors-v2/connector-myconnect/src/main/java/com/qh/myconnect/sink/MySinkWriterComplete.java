package com.qh.myconnect.sink;

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

    public MySinkWriterComplete(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, Long tableCount) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.context = context;
        this.currentTaskId=context.getIndexOfSubtask();
        log.info("currentTaskId:"+this.currentTaskId);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.table = this.jdbcSinkConfig.getTable();
        this.preConfig = jdbcSinkConfig.getPreConfig();
        this.startTime = LocalDateTime.now();
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);
        this.tableCount = tableCount;
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
        if (this.writeCount == 1) {
            if (this.preConfig.getInsertMode().equalsIgnoreCase("complete")) {
                if (this.preConfig.isCleanTableWhenComplete() && !this.preConfig.isCleanTableWhenCompleteNoDataIn()) {
                    TruncateTable truncateTable = new TruncateTable();
                    truncateTable.setFlinkJobId(this.jobContext.getJobId());
                    truncateTable.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
                    if (this.jdbcSinkConfig.getDbSchema() != null && !this.jdbcSinkConfig.getDbSchema().equalsIgnoreCase("")) {
                        truncateTable.setDbSchema(this.jdbcSinkConfig.getDbSchema());
                        truncateTable.setTableName(this.jdbcSinkConfig.getDbSchema() + "." + this.jdbcSinkConfig.getTable());
                    } else {
                        truncateTable.setTableName(this.jdbcSinkConfig.getTable());
                    }
                    util.truncateTable(truncateTable);
                }
            }
        }
        this.cld.add(element);
        if (this.writeCount.longValue() % batchSize == 0) {
            this.insertToDb();
            cld.clear();
        }
    }

    @Override
    public void close() {
        this.insertToDb();
        try {
            conn.close();
            statisticalResults();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void insertToDb() {
        try (Connection conn = DriverManager.getConnection(this.jdbcSinkConfig.getUrl(), this.jdbcSinkConfig.getUser(), this.jdbcSinkConfig.getPassWord())) {
            List<String> columns = this.columnMappers.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = jdbcDialect.insertTableSql(this.jdbcSinkConfig, columns, values);
            PreparedStatement psUpsert = conn.prepareStatement(sql);
            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = seaTunnelRow.getField(valueIndex);
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    psUpsert.addBatch();
                }
            }
            psUpsert.executeBatch();
            psUpsert.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void statisticalResults() throws Exception {
        LocalDateTime endTime = LocalDateTime.now();
        Long deleteCount = 0L;
        try {
            if (this.currentTaskId == 0) {
                deleteCount = this.tableCount;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StatisticalLog statisticalLog = new StatisticalLog();
        statisticalLog.setFlinkJobId(this.jobContext.getJobId());
        statisticalLog.setWriteCount(writeCount);
        statisticalLog.setModifyCount(0L);
        statisticalLog.setDeleteCount(deleteCount);
        statisticalLog.setInsertCount(writeCount);
        statisticalLog.setKeepCount(0L);
        statisticalLog.setStartTime(startTime);
        statisticalLog.setEndTime(endTime);
        util.insertLog(statisticalLog);
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
