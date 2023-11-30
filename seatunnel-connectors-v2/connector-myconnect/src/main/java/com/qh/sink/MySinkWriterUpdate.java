package com.qh.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qh.config.JdbcSinkConfig;
import com.qh.config.Util;
import com.qh.converter.ColumnMapper;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.json.JSONObject;
import org.stringtemplate.v4.ST;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;


@Slf4j
public class MySinkWriterUpdate extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private SeaTunnelRowType sinkTableRowType;
    private List<SeaTunnelRow> cld = new ArrayList<>();
    private LongAdder writeCount = new LongAdder();
    private LongAdder keepCount = new LongAdder();
    private LongAdder updateCount = new LongAdder();
    private LongAdder deleteCount = new LongAdder();
    private LongAdder insertCount = new LongAdder();
    //切记不要换成hashmap

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

    private Util util = new Util();

    public MySinkWriterUpdate(SeaTunnelRowType seaTunnelRowType,
                              Context context,
                              ReadonlyConfig config,
                              JobContext jobContext,
                              LocalDateTime startTime) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.startTime = startTime;
        this.table = this.jdbcSinkConfig.getTable();
        this.tmpTable = "UC_" + this.jdbcSinkConfig.getTable();
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);

        String sqlQuery = jdbcDialect.getSinkQueryUpdate(this.table, this.columnMappers, 0);
        PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
        ResultSet resultSet = preparedStatementQuery.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> metaDataHash = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            metaDataHash.put(metaData.getColumnName(i + 1), metaData.getColumnTypeName(i + 1));
        }
        this.metaDataHash = metaDataHash;
        preparedStatementQuery.close();


        String redisKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable());
        Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
        jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
        jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
        jedis.incr(redisKey);
        jedis.disconnect();


    }

    private void consumeData() {
        HashMap<List<String>, SeaTunnelRow> sourceRows = new HashMap<>();
        List<SeaTunnelRow> oldRows = new ArrayList<>();
        List<ColumnMapper> ucColumns = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        for (SeaTunnelRow seaTunnelRow : this.cld) {
            if (seaTunnelRow != null) {
                oldRows.add(seaTunnelRow);
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
            insertTmpUks(oldRows, this.metaDataHash, conn);
            compareData(sourceRows);
        } catch (SQLException e) {
            Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
            jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
            jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
            String redisKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable());
            String redisListKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable()) + ":list";
            jedis.del(redisKey);
            jedis.del(redisListKey);
            throw new RuntimeException(e);
        }

    }

    private void compareData(HashMap<List<String>, SeaTunnelRow> sourceRows) throws SQLException {
        HashMap<List<String>, SeaTunnelRow> sinkRows = getSinkRows(conn, sourceRows);
        List<SeaTunnelRow> needInsertRows = new ArrayList<>();
        HashMap<List<String>, SeaTunnelRow> needUpdate = new HashMap<>();
        sourceRows.forEach((k, sourceRow) -> {
            SeaTunnelRow sinkRow = sinkRows.get(k);
            if (null != sinkRow) {
                if (!sourceRow.equals(sinkRow)) {
                    needUpdate.put(k, sourceRow);
                    this.updateCount.increment();
                } else {
                    this.keepCount.increment();
                }
            } else {
                needInsertRows.add(sourceRow);
                this.insertCount.increment();
            }
        });
        doInsert(needInsertRows, this.metaDataHash, conn);
        doUpdate(needUpdate, this.metaDataHash, conn);

    }

    private void initColumnMappers(JdbcSinkConfig jdbcSinkConfig, SeaTunnelRowType sourceRowType, SeaTunnelRowType sinkTableRowType, Connection conn) throws SQLException {
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
                PreparedStatement preparedStatementQuery = conn.prepareStatement("select  * from " + table + " where 1=2 ");
                ResultSet resultSet = preparedStatementQuery.executeQuery();
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    if (v.equalsIgnoreCase(columnName)) {
                        String columnTypeName = metaData.getColumnTypeName(i + 1);
                        columnMapper.setSinkColumnDbType(columnTypeName);
                    }
                }
                preparedStatementQuery.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            columnMappers.add(columnMapper);
        });
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        this.writeCount.increment();
        this.cld.add(element);
        if (this.writeCount.longValue() % batchSize == 0) {
            this.consumeData();
            cld.clear();
        }
    }

    private void doInsert(List<SeaTunnelRow> rows, Map<String, String> metaDataHash, Connection connection) throws SQLException {
        List<String> newColumns = new ArrayList<>();
        for (ColumnMapper column : columnMappers) {
            newColumns.add(column.getSinkColumnName());
        }
        String templateInsert = "INSERT INTO <table> " +
                "(<columns:{sub | <sub>}; separator=\", \">) " +
                "values(  <columns:{sub | ?}; separator=\", \"> ) ";
        ST template = new ST(templateInsert);
        template.add("table", table);
        template.add("columns", newColumns);
        String insertSql = template.render();
        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
        for (SeaTunnelRow row : rows) {
            for (int i = 0; i < columnMappers.size(); i++) {
                String column = newColumns.get(i);
                String dbType = metaDataHash.get(column);
                jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, (String) row.getField(i));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    private void doUpdate(HashMap<List<String>, SeaTunnelRow> rows, Map<String, String> metaDataHash, Connection connection) throws SQLException {
        List<ColumnMapper> listUc = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        this.jdbcDialect.updateData(conn, table, columnMappers, listUc, rows, metaDataHash);

    }

    private HashMap<List<String>, SeaTunnelRow> getSinkRows(Connection conn, HashMap<List<String>, SeaTunnelRow> sourceRows) {
        AtomicInteger tmp = new AtomicInteger();
        HashMap<List<String>, SeaTunnelRow> sinkRows = new HashMap<>();
        String sqlQuery = jdbcDialect.getSinkQueryUpdate(this.table, this.columnMappers, sourceRows.size());
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
                ;
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
        String redisKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable());
        Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
        jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
        jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
        jedis.decr(redisKey);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("writeCount", writeCount);
        jsonObject.put("keepCount", keepCount);
        jsonObject.put("updateCount", updateCount);
        jsonObject.put("insertCount", insertCount);

        String redisListKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable()) + ":list";
        jedis.lpush(redisListKey, jsonObject.toString());
        String value = jedis.get(redisKey);
        if (null != value) {
            int i = Integer.parseInt(value);
            if (i == 0) {
                long running = jedis.setnx(redisKey + ":running", "value");
                jedis.expire(redisKey + ":running", 5);
                if (running == 1) {
                    List<String> totalList = jedis.lrange(redisListKey, 0, -1);
                    writeCount.reset();
                    keepCount.reset();
                    updateCount.reset();
                    insertCount.reset();
                    for (String s : totalList) {
                        ObjectMapper objectMapper = new ObjectMapper();
                        try {
                            JsonNode jsonNode = objectMapper.readTree(s);
                            writeCount.add(jsonNode.get("writeCount").asLong());
                            keepCount.add(jsonNode.get("keepCount").asLong());
                            updateCount.add(jsonNode.get("updateCount").asLong());
                            insertCount.add(jsonNode.get("insertCount").asLong());
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    List<ColumnMapper> ucColumns = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
                    int del = this.jdbcDialect.deleteData(conn, table, tmpTable, ucColumns);
                    deleteCount.add(del);
                    LocalDateTime endTime = LocalDateTime.now();
                    util.insertLog(writeCount.longValue(),
                            updateCount.longValue(),
                            deleteCount.longValue(),
                            keepCount.longValue(),
                            insertCount.longValue(),
                            this.jobContext.getJobId(),
                            startTime,
                            endTime);

                    jedis.del(redisKey);
                    jedis.del(redisListKey);
                }
            }
        }

        jedis.disconnect();


    }

    private void insertTmpUks(List<SeaTunnelRow> oldRows, Map<String, String> metaDataHash, Connection conn) throws SQLException {
        List<ColumnMapper> ucColumns = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String insert = String.format("insert  into %s(%s) values(%s)",
                tmpTable,
                StringUtils.join(ucColumns.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList()), ","),
                StringUtils.join(ucColumns.stream().map(x -> "?").collect(Collectors.toList()), ",")
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

    }

    @Override
    public void close() {
        try {
            this.consumeData();
            statisticalResults(conn);
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
