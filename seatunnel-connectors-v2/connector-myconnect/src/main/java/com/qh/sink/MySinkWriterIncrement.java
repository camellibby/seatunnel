package com.qh.sink;

import com.mysql.cj.jdbc.Driver;
import com.qh.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.stringtemplate.v4.ST;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;


@Slf4j
public class MySinkWriterIncrement extends AbstractSinkWriter<SeaTunnelRow, Void> {
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


    public MySinkWriterIncrement(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, boolean upsert) throws SQLException {
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

        this.columnNameAndIndex.put("XJ_ST_FLAG", -1);
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
            List<String> columnsWithOutXjFlag = new ArrayList<>();
            for (String column : columns) {
                if (!column.equalsIgnoreCase("XJ_ST_FLAG")) {
                    columnsWithOutXjFlag.add(column);
                }
            }
            String table = this.jdbcSinkConfig.getTable();
            List<String> primaryKeys = new ArrayList<>();

            DatabaseMetaData metadata = conn.getMetaData();
            ResultSet rsPk = metadata.getPrimaryKeys(null, null, table);
            while (rsPk.next()) {
                String columnName = rsPk.getString("COLUMN_NAME");
                primaryKeys.add(columnName);
            }
            String templateInsert = "INSERT INTO <table> " +
                    "(<columns:{sub | <sub>}; separator=\", \">) " +
                    "select  <columns:{sub | ?}; separator=\", \"> from dual " +
                    " where not EXISTS (SELECT 1 FROM <table> WHERE  <pks:{pk | <pk> = ? }; separator=\" and \"> )";
            ST template = new ST(templateInsert);
            template.add("table", table);
            template.add("columns", columns);
            template.add("pks", primaryKeys);
            String insertSql = template.render();
            PreparedStatement psInsert = conn.prepareStatement(insertSql);


            String templateKeep = "update <table> set XJ_ST_FLAG='K' " +
                    " where <columns:{sub | <sub> = ? }; separator=\" and \"> and XJ_ST_FLAG is null";
            ST template2 = new ST(templateKeep);
            template2.add("table", table);
            template2.add("columns", columnsWithOutXjFlag);
            String keepSql = template2.render();
            PreparedStatement psKeep = conn.prepareStatement(keepSql);


            String templateModify = "update <table> set <columns:{sub | <sub> = ? }; separator=\" , \"> ," +
                    " XJ_ST_FLAG='M' " +
                    "where  <pks:{pk | <pk> = ? }; separator=\" and \"> and XJ_ST_FLAG is null";
            ST template3 = new ST(templateModify);
            template3.add("table", table);
            template3.add("columns", columnsWithOutXjFlag);
            template3.add("pks", primaryKeys);
            String keepModify = template3.render();
            PreparedStatement psModify = conn.prepareStatement(keepModify);

            int size = 0;
            while (!stop || !cld.isEmpty()) {
                SeaTunnelRow poll = cld.poll();
                if (poll != null) {
                    //直接插入 XJ_ST_FLAG=I
                    psSetValue(columns, psInsert, poll);
                    int valueSize = columns.size();
                    for (int i = 0; i < primaryKeys.size(); i++) {
                        Integer valueIndex = this.columnNameAndIndex.get(primaryKeys.get(i));
                        Object field = poll.getField(valueIndex);
                        if (field instanceof Date) {
                            psInsert.setTimestamp(valueSize + i + 1, new Timestamp(((Date) field).getTime()));
                        } else if (field instanceof LocalDate) {
                            psInsert.setDate(valueSize + i + 1, java.sql.Date.valueOf((java.time.LocalDate) field));
                        } else if (field instanceof Integer) {
                            psInsert.setInt(valueSize + i + 1, (Integer) field);
                        } else if (field instanceof Long) {
                            psInsert.setLong(valueSize + i + 1, (Long) field);
                        } else if (field instanceof Double) {
                            psInsert.setDouble(valueSize + i + 1, (Double) field);
                        } else if (field instanceof Float) {
                            psInsert.setFloat(valueSize + i + 1, (Float) field);
                        } else {
                            psInsert.setString(valueSize + i + 1, (String) field);
                        }
                    }
                    // 找到没改的 XJ_ST_FLAG=K
                    psSetValue(columnsWithOutXjFlag, psKeep, poll);

                    // 找到改了的 XJ_ST_FLAG=M
                    psSetValue(columnsWithOutXjFlag, psModify, poll);
                    int valueSize2 = columnsWithOutXjFlag.size();
                    for (int i = 0; i < primaryKeys.size(); i++) {
                        Integer valueIndex = this.columnNameAndIndex.get(primaryKeys.get(i));
                        Object field = poll.getField(valueIndex);
                        if (field instanceof Date) {
                            psModify.setTimestamp(valueSize2 + i + 1, new Timestamp(((Date) field).getTime()));
                        } else if (field instanceof LocalDate) {
                            psModify.setDate(valueSize2 + i + 1, java.sql.Date.valueOf((java.time.LocalDate) field));
                        } else if (field instanceof Integer) {
                            psModify.setInt(valueSize2 + i + 1, (Integer) field);
                        } else if (field instanceof Long) {
                            psModify.setLong(valueSize2 + i + 1, (Long) field);
                        } else if (field instanceof Double) {
                            psModify.setDouble(valueSize2 + i + 1, (Double) field);
                        } else if (field instanceof Float) {
                            psModify.setFloat(valueSize2 + i + 1, (Float) field);
                        } else {
                            psModify.setString(valueSize2 + i + 1, (String) field);
                        }
                    }
                    size++;
                    psInsert.addBatch();
                    psKeep.addBatch();
                    psModify.addBatch();
                    psInsert.clearParameters();
                    psKeep.clearParameters();
                    psModify.clearParameters();
                }
                if (size == 1000) {
                    psInsert.executeBatch();
                    psKeep.executeBatch();
                    psModify.executeBatch();
                    psInsert.clearBatch();
                    psKeep.clearBatch();
                    psModify.clearBatch();
                    size = 0;
                }
            }
            psInsert.executeBatch();
            psKeep.executeBatch();
            psModify.executeBatch();
            this.columnNameAndIndex.put("XJ_ST_FLAG", -1);

            String redisKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable());
            String redisListKey = String.format("seatunnel:job:sink:%s", jdbcSinkConfig.getTable()) + ":list";
            Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
            jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
            jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
            jedis.decr(redisKey);
            jedis.lpush(redisListKey, String.valueOf(longAdder.sum()));
            jedis.disconnect();
            statisticalResults(redisKey, redisListKey, table);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void psSetValue(List<String> columns, PreparedStatement preparedStatement, SeaTunnelRow poll) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            Integer valueIndex = this.columnNameAndIndex.get(columns.get(i));
            if (valueIndex != -1) {
                Object field = poll.getField(valueIndex);
                if (field instanceof Date) {
                    preparedStatement.setTimestamp(i + 1, new Timestamp(((Date) field).getTime()));
                } else if (field instanceof LocalDate) {
                    preparedStatement.setDate(i + 1, java.sql.Date.valueOf((LocalDate) field));
                } else if (field instanceof Integer) {
                    preparedStatement.setInt(i + 1, (Integer) field);
                } else if (field instanceof Long) {
                    preparedStatement.setLong(i + 1, (Long) field);
                } else if (field instanceof Double) {
                    preparedStatement.setDouble(i + 1, (Double) field);
                } else if (field instanceof Float) {
                    preparedStatement.setFloat(i + 1, (Float) field);
                } else {
                    preparedStatement.setString(i + 1, (String) field);
                }
            } else {
                preparedStatement.setString(i + 1, "I");
            }
        }
    }

    public void statisticalResults(String redisKey, String redisListKey, String table) {
        try (Connection conn = DriverManager.getConnection(this.jdbcSinkConfig.getUrl(), this.jdbcSinkConfig.getUser(), this.jdbcSinkConfig.getPassWord())) {
            Jedis jedis = new Jedis(this.jdbcSinkConfig.getPreConfig().getRedisHost(), this.jdbcSinkConfig.getPreConfig().getRedisPort());
            jedis.auth(this.jdbcSinkConfig.getPreConfig().getRedisPassWord());
            jedis.select(this.jdbcSinkConfig.getPreConfig().getRedisDbIndex());
            String value = jedis.get(redisKey);
            if (null != value) {
                int i = Integer.parseInt(value);
                System.out.println(i + "------------" + Thread.currentThread().getName());
                if (i == 0) {
                    long running = jedis.setnx(redisKey + ":running", "value");
                    jedis.expire(redisKey + ":running", 5);
                    if (running == 1) {
                        List<String> writeCountList = jedis.lrange(redisListKey, 0, -1);
                        Long writeCount = writeCountList.stream().map(Long::parseLong).reduce(Long::sum).orElse(0L);
                        jedis.del(redisKey);
                        jedis.del(redisListKey);
                        jedis.disconnect();
                        String templateInsert = " select " +
                                "       (select count(1) from <table> where XJ_ST_FLAG = 'I') insertCount," +
                                "       (select count(1) from <table> where XJ_ST_FLAG = 'M') modifyCount," +
                                "       (select count(1) from <table> where XJ_ST_FLAG = 'K') KeepCount," +
                                "       (select count(1) from <table> where XJ_ST_FLAG IS NULL) deleteCount" +
                                " from DUAL";
                        ST template = new ST(templateInsert);
                        template.add("table", table);
                        String sql = template.render();
                        PreparedStatement ps = conn.prepareStatement(sql);
                        ResultSet resultSet = ps.executeQuery();
                        String templateDelete = "delete from <table> where XJ_ST_FLAG IS NULL";
                        ST template2 = new ST(templateDelete);
                        template2.add("table", table);
                        String delSql = template2.render();
                        PreparedStatement psDelete = conn.prepareStatement(delSql);
                        psDelete.execute();
                        while (resultSet.next()) {
                            long insertCount = resultSet.getLong("insertCount");
                            long modifyCount = resultSet.getLong("modifyCount");
                            long keepCount = resultSet.getLong("KeepCount");
                            long deleteCount = resultSet.getLong("deleteCount");
                            insertLog(writeCount, modifyCount, deleteCount, keepCount, insertCount, this.jobContext.getJobId());
                        }
                    }
                }
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
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
