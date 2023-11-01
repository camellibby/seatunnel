package com.qh.sink;

import com.mysql.cj.jdbc.Driver;
import com.qh.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.stringtemplate.v4.ST;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;


@Slf4j
public class MySinkWriterComplete extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final Context context;
    private ConcurrentLinkedDeque<SeaTunnelRow> cld = new ConcurrentLinkedDeque<SeaTunnelRow>();
    private LongAdder longAdder = new LongAdder();
    private final Map<String, Integer> columnNameAndIndex = new HashMap<>();
    private final JdbcSinkConfig jdbcSinkConfig;
    private boolean upsert = false;
    private JobContext jobContext;
    private volatile boolean stop = false;

    private LocalDateTime startTime;

    public MySinkWriterComplete(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, boolean upsert) throws SQLException {
        this.jobContext = jobContext;
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.upsert = upsert;
        this.startTime = LocalDateTime.now();
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        fieldMapper.forEach((k, v) -> {
                    for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                        if (seaTunnelRowType.getFieldNames()[i].equalsIgnoreCase(k)) {
                            this.columnNameAndIndex.put(v, i);
                        }
                    }
                }
        );

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
            List<String> columns = new ArrayList<>(this.columnNameAndIndex.keySet());
            String table = this.jdbcSinkConfig.getTable();
            List<String> values = this.columnNameAndIndex.entrySet().stream().map(x -> "?").collect(Collectors.toList());
            String sqlUpsert = "insert into "
                    + table
                    + String.format("(%s)", StringUtils.join(columns, ","))
                    + String.format("values (%s)", StringUtils.join(values, ","));
            PreparedStatement psUpsert = conn.prepareStatement(sqlUpsert);
            int size = 0;
            while (!stop || !cld.isEmpty()) {
                SeaTunnelRow poll = cld.poll();
                if (poll != null) {
                    for (int i = 0; i < columns.size(); i++) {
                        Integer valueIndex = this.columnNameAndIndex.get(columns.get(i));
                        Object field = poll.getField(valueIndex);
                        if (field instanceof Date) {
                            psUpsert.setTimestamp(i + 1, new Timestamp(((Date) field).getTime()));
                        } else if (field instanceof LocalDate) {
                            psUpsert.setDate(i + 1, java.sql.Date.valueOf((LocalDate) field));
                        } else if (field instanceof Integer) {
                            psUpsert.setInt(i + 1, (Integer) field);
                        } else if (field instanceof Long) {
                            psUpsert.setLong(i + 1, (Long) field);
                        } else if (field instanceof Double) {
                            psUpsert.setDouble(i + 1, (Double) field);
                        } else if (field instanceof Float) {
                            psUpsert.setFloat(i + 1, (Float) field);
                        } else {
                            psUpsert.setString(i + 1, (String) field);
                        }
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
                    insertLog(writeCount, 0L, writeCount, 0L, writeCount, this.jobContext.getJobId());
                }
            }
        }
    }

    public void insertLog(Long writeCount, Long modifyCount, Long deleteCount, Long keepCount, Long insertCount, String flinkJobId) {
        String user = System.getenv("MYSQL_MASTER_USER");
        String password = System.getenv("MYSQL_MASTER_PWD");
        String dbHost = System.getenv("MYSQL_MASTER_HOST");
        String dbPort = System.getenv("MYSQL_MASTER_PORT");
        String dbName = System.getenv("PANGU_DB");
        String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        String querySql = "select  count(1) logCount from  seatunnel_jobs_history where flinkJobId=?";
        try (Connection connection = new Driver().connect(url, info)) {
            PreparedStatement statement = connection.prepareStatement(querySql);
            statement.setString(1, this.jobContext.getJobId());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int logCount = resultSet.getInt("logCount");
                if (logCount == 1) {
                    String updateString = "update seatunnel_jobs_history  " +
                            "   SET writeCount  = <writeCount>,  " +
                            "       updateCount = <updateCount>,  " +
                            "       deleteCount = <deleteCount>,  " +
                            "       keepCount   = <keepCount>,  " +
                            "       insertCount = <insertCount>  " +
                            " WHERE flinkJobId = '<flinkJobId>'";
                    ST template = new ST(updateString);
                    template.add("writeCount", writeCount);
                    template.add("updateCount", modifyCount);
                    template.add("deleteCount", deleteCount);
                    template.add("keepCount", keepCount);
                    template.add("insertCount", insertCount);
                    template.add("flinkJobId", flinkJobId);
                    String updateSql = template.render();
                    PreparedStatement psUpdate = connection.prepareStatement(updateSql);
                    psUpdate.execute();
                } else {
                    LocalDateTime endTime = LocalDateTime.now();
                    String insertString = "INSERT INTO seatunnel_jobs_history  " +
                            "  (jobId,  " +
                            "   flinkJobId,  " +
                            "   jobStatus,  " +
                            "   startTime,  " +
                            "   endTime,  " +
                            "   writeCount,  " +
                            "   updateCount,  " +
                            "   deleteCount,  " +
                            "   keepCount,  " +
                            "   insertCount )  " +
                            "VALUES  " +
                            "  ('<jobId>',  " +
                            "   '<flinkJobId>',  " +
                            "   'FINISHED',  " +
                            "   '<startTime>',  " +
                            "   '<endTime>',  " +
                            "   <writeCount>,  " +
                            "   <updateCount>,  " +
                            "   <deleteCount>,  " +
                            "   <keepCount>,  " +
                            "   <insertCount>)";
                    ST template = new ST(insertString);
                    template.add("jobId", flinkJobId);
                    template.add("flinkJobId", flinkJobId);
                    template.add("writeCount", writeCount);
                    template.add("updateCount", modifyCount);
                    template.add("deleteCount", deleteCount);
                    template.add("keepCount", keepCount);
                    template.add("insertCount", insertCount);
                    template.add("startTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    template.add("endTime", endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    String insertSql = template.render();
                    PreparedStatement psInsert = connection.prepareStatement(insertSql);
                    psInsert.execute();

                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
