package com.qh.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qh.config.JdbcSinkConfig;
import com.qh.converter.ColumnMapper;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.oracle.OracleDialect;
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
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static com.qh.sink.Utils.*;


@Slf4j
public class MySinkWriterUpdate extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private SeaTunnelRowType sinkTableRowType;
    private ConcurrentLinkedDeque<SeaTunnelRow> cld = new ConcurrentLinkedDeque<SeaTunnelRow>();
    private LongAdder writeCount = new LongAdder();
    private LongAdder keepCount = new LongAdder();
    private LongAdder updateCount = new LongAdder();
    private LongAdder deleteCount = new LongAdder();
    private LongAdder insertCount = new LongAdder();
    //切记不要换成hashmap

    private final JdbcSinkConfig jdbcSinkConfig;
    private JobContext jobContext;
    private volatile boolean stop = false;
    private LocalDateTime startTime;
    private int batchSize = 1000;
    private JdbcDialect jdbcDialect;
    private String table;
    private String tmpTable;

    private Map<String, String> metaDataHash;

    private Connection conn;

    private List<ColumnMapper> columnMappers = new ArrayList<>();

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
        if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("ORACLE")) {
            this.jdbcDialect = new OracleDialect();
        }
        this.conn = DriverManager.getConnection(this.jdbcSinkConfig.getUrl(), this.jdbcSinkConfig.getUser(), this.jdbcSinkConfig.getPassWord());
        this.sinkTableRowType = initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
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

        new Thread(this::consumeData).start();

    }

    private void consumeData() {
        int size = 0;
        List<SeaTunnelRow> oldRows = new ArrayList<>();
        HashMap<List<String>, SeaTunnelRow> sourceRows = new HashMap<>();
        List<ColumnMapper> ucColumns = this.columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        while (!stop || !cld.isEmpty()) {
            SeaTunnelRow poll = cld.poll();
            if (poll != null) {
                oldRows.add(poll);
                SeaTunnelRow newRow = new SeaTunnelRow(columnMappers.size());
                for (int i = 0; i < this.columnMappers.size(); i++) {
                    Object field = poll.getField(this.columnMappers.get(i).getSourceRowPosition());
                    newRow.setField(i, Object2String(field));
                }
                List<String> keys = new ArrayList<>();
                ucColumns.forEach(x -> {
                    Object field = poll.getField(x.getSourceRowPosition());
                    keys.add(Object2String(field));
                });
                sourceRows.put(keys, newRow);
                size++;
            }
            if (size == batchSize) {
                try {
                    insertTmpUks(oldRows, this.metaDataHash, conn);
                    compareData(sourceRows);
                    oldRows.clear();
                    sourceRows.clear();
                    size = 0;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            insertTmpUks(oldRows, this.metaDataHash, conn);
            compareData(sourceRows);
            oldRows.clear();
            sourceRows.clear();
            statisticalResults(conn);
            conn.close();
        } catch (SQLException e) {
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
        List<UniqueConstraint> uniqueConstraints = getUniqueConstraints(conn, null, this.table);
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

            for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                if (uniqueConstraint.columns.contains(v)) {
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
        cld.add(element);
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

        String templateInsert = "update <table> set " +
                "<columns:{sub | <sub.sinkColumnName> = ? }; separator=\", \"> " +
                " where  <pks:{pk | <pk.sinkColumnName> = ? }; separator=\" and \"> ";
        ST template = new ST(templateInsert);
        template.add("table", table);
        template.add("columns", columnMappers);
        template.add("pks", listUc);
        String insertSql = template.render();
        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
        for (SeaTunnelRow row : rows.values()) {
            for (int i = 0; i < columnMappers.size(); i++) {
                String column = columnMappers.get(i).getSinkColumnName();
                String dbType = metaDataHash.get(column);
                System.out.println(column);
                jdbcDialect.setPreparedStatementValueByDbType(
                        i + 1,
                        preparedStatement,
                        dbType,
                        (String) row.getField(columnMappers.get(i).getSinkRowPosition()));
            }
            for (int i = 0; i < listUc.size(); i++) {
                String column = listUc.get(i).getSinkColumnName();
                String dbType = metaDataHash.get(column);
                jdbcDialect.setPreparedStatementValueByDbType(
                        i + 1 + columnMappers.size(),
                        preparedStatement,
                        dbType,
                        (String) row.getField(listUc.get(i).getSinkRowPosition()));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
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
                jdbcDialect.setPreparedStatementValueByDbType(i + 1, preparedStatement, dbType, Object2String(oldRow.getField(ucColumns.get(i).getSourceRowPosition())));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();

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
                SeaTunnelRow internal = toInternal(resultSet, this.sinkTableRowType);
                SeaTunnelRow newRow = new SeaTunnelRow(columnMappers.size());
                for (int i = 0; i < this.columnMappers.size(); i++) {
                    Object field = internal.getField(columnMappers.get(i).getSinkRowPosition());
                    newRow.setField(i, Object2String(field));
                }
                List<String> keys = new ArrayList<>();
                ucColumns.forEach((x -> {
                    Object field = internal.getField(x.getSinkRowPosition());
                    keys.add(Object2String(field));
                }));
                sinkRows.put(keys, newRow);
            }
            preparedStatementQuery.close();
            return sinkRows;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void statisticalResults(Connection conn) throws SQLException {
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
                    String delSql = "delete from  <table> a   " +
                            " where not exists " +
                            "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> b where <pks:{pk | a.<pk.sinkColumnName>=b.<pk.sinkColumnName> }; separator=\" and \">  ) ";
                    ST template = new ST(delSql);
                    template.add("table", table);
                    template.add("tmpTable", tmpTable);
                    template.add("pks", ucColumns);
                    String render = template.render();
                    PreparedStatement preparedStatement = conn.prepareStatement(render);
                    int del = preparedStatement.executeUpdate();
                    preparedStatement.close();
                    deleteCount.add(del);
                    LocalDateTime endTime = LocalDateTime.now();
                    insertLog(writeCount.longValue(),
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

    @Override
    public void close() {
        this.stop = true;
    }


}
