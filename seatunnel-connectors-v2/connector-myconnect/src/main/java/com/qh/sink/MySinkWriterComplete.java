package com.qh.sink;

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
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static com.qh.sink.Utils.*;


@Slf4j
public class MySinkWriterComplete extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private final Context context;
    private ConcurrentLinkedDeque<SeaTunnelRow> cld = new ConcurrentLinkedDeque<SeaTunnelRow>();
    private LongAdder longAdder = new LongAdder();
    private final JdbcSinkConfig jdbcSinkConfig;
    private JobContext jobContext;
    private volatile boolean stop = false;

    private JdbcDialect jdbcDialect;

    private LocalDateTime startTime;
    private Map<String, String> metaDataHash;

    private Connection conn;

    private String table;
    private List<ColumnMapper> columnMappers = new ArrayList<>();

    private SeaTunnelRowType sinkTableRowType;

    public MySinkWriterComplete(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.context = context;
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.table = this.jdbcSinkConfig.getTable();
        this.startTime = LocalDateTime.now();

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

        Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
        jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
        jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
        jedis.incr(String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable()));
        jedis.expire(String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable()), 36000);
        jedis.disconnect();
        new Thread(this::insertToDb).start();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        this.longAdder.increment();
        cld.add(element);
    }

    @Override
    public void close() {
        this.stop = true;
    }

    public void insertToDb() {
        try (Connection conn = DriverManager.getConnection(this.jdbcSinkConfig.getUrl(), this.jdbcSinkConfig.getUser(), this.jdbcSinkConfig.getPassWord())) {
            List<String> columns = this.columnMappers.stream().map(x->x.getSinkColumnName()).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sqlUpsert = "insert into "
                    + table
                    + String.format("(%s)", StringUtils.join(columns, ","))
                    + String.format("values (%s)", StringUtils.join(values, ","));
            PreparedStatement psUpsert = conn.prepareStatement(sqlUpsert);
            int size = 0;
            while (!stop || !cld.isEmpty()) {
                SeaTunnelRow poll = cld.poll();
                if (poll != null) {
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex =this.columnMappers.get(i).getSourceRowPosition();
                        Object field = poll.getField(valueIndex);
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, Object2String(field));
                    }
                    size++;
                    psUpsert.addBatch();
                    psUpsert.clearParameters();
                }
                if (size == 1000) {
                    psUpsert.executeBatch();
                    size = 0;
                    psUpsert.clearBatch();
                }
            }

            psUpsert.executeBatch();
            String redisKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable());
            String redisListKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable()) + ":list";
            Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
            jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
            jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
            jedis.decr(redisKey);
            jedis.lpush(redisListKey, String.valueOf(longAdder.sum()));
            jedis.disconnect();
            statisticalResults(redisKey, redisListKey);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void statisticalResults(String redisKey, String redisListKey) {
        Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
        jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
        jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
        String value = jedis.get(redisKey);
        if (null != value) {
            int i = Integer.parseInt(value);
            if (i == 0) {
                long running = jedis.setnx(redisKey + ":running", "value");
                jedis.expire(redisKey + ":running", 5);
                if (running == 1) {
                    List<String> writeCountList = jedis.lrange(redisListKey, 0, -1);
                    Long writeCount = writeCountList.stream().map(Long::parseLong).reduce(Long::sum).orElse(0L);
                    jedis.del(redisKey);
                    jedis.del(redisListKey);
                    jedis.disconnect();
                    LocalDateTime endTime = LocalDateTime.now();
                    insertLog(writeCount, 0L, writeCount, 0L, writeCount, this.jobContext.getJobId(), startTime, endTime);
                }
            }
        }
        jedis.disconnect();
        try {
            conn.close();
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
            String typeNameSS = sourceRowType.getFieldType(sourceRowType.indexOf(k)).getTypeClass().getName();
            columnMapper.setSourceColumnTypeName(typeNameSS);

            columnMapper.setSinkColumnName(v);
            columnMapper.setSinkRowPosition(sinkTableRowType.indexOf(v));
            String typeNameSK = sinkTableRowType.getFieldType(sinkTableRowType.indexOf(v)).getTypeClass().getName();
            columnMapper.setSinkColumnTypeName(typeNameSK);
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


}
