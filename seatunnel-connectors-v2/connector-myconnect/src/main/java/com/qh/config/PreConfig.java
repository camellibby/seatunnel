package com.qh.config;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.util.OptionMark;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Data
public class PreConfig implements Serializable {
    private static final long serialVersionUID = -1L;
    @OptionMark(description = "插入模式")
    private String insertMode;//插入模式 全量 complete  增量 increment
    @OptionMark(description = "全量模式是否清空表 true清 false不清")
    private boolean cleanTableWhenComplete;
    @OptionMark(description = "增量模式 update或者zipper模式 ")
    private String incrementMode;
    private static final List<String> zipperColumns = Arrays.asList("zipperFlag", "zipperTime");

    private String redisHost;
    private Integer redisPort;
    private String redisPassWord;
    private Integer redisDbIndex;


    public PreConfig() {
        String redisHost = System.getenv("REDISHOST");
        String redisPort = System.getenv("REDISPORT");
        String redisPassword = System.getenv("REDISPASSWORD");
        String redisDbIndex = System.getenv("REDISDBINDEX");
        this.redisHost = redisHost;
        this.redisPort = Integer.valueOf(redisPort);
        this.redisPassWord = redisPassword;
        this.redisDbIndex = Integer.valueOf(redisDbIndex);
    }


    public void doPreConfig(Connection connection, String tableName) throws SQLException {
        Jedis jedis = new Jedis(this.redisHost, this.redisPort);
        jedis.auth(this.redisPassWord);
        jedis.select(this.redisDbIndex);
        String values = jedis.get(String.format("seatunnel:job:sink:%s", tableName));
        if (null != values) {
            jedis.disconnect();
            connection.close();
            throw new RuntimeException(String.format("有作业正在往表%s写入数据，作业禁止运行", tableName));
        }
        jedis.disconnect();


        DatabaseMetaData metadata = connection.getMetaData();
        ResultSet rsColumn = metadata.getColumns(null, null, tableName, null);
        List<String> columns = new ArrayList<>();
        while (rsColumn.next()) {
            String name = rsColumn.getString("COLUMN_NAME");
            columns.add(name);
            String type = rsColumn.getString("TYPE_NAME");
            int size = rsColumn.getInt("COLUMN_SIZE");
        }
        ResultSet rsPk = metadata.getPrimaryKeys(null, null, tableName);
        List<String> primaryKeys = new ArrayList<>();
        while (rsPk.next()) {
            String columnName = rsPk.getString("COLUMN_NAME");
            primaryKeys.add(columnName);
        }
        List<String> uniqueIndex = new ArrayList<>();
        try (ResultSet rs = metadata.getIndexInfo(null, null, tableName, true, true)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                if (indexName == null || columnName == null) {
                    continue;
                }
                uniqueIndex.add(columnName);
            }
        }


        if (this.insertMode.equalsIgnoreCase("complete")) {
            if (this.cleanTableWhenComplete) {
                Statement st = connection.createStatement();
                st.execute(String.format("truncate  table %s", tableName));
            }
        }
        if (this.insertMode.equalsIgnoreCase("increment")) {
            if (this.incrementMode.equalsIgnoreCase("update")) {
                if (primaryKeys.isEmpty() && uniqueIndex.isEmpty()) {
                    throw new RuntimeException(String.format("增量更新模式下,目标表(%s)必须包含主键或者唯一索引", tableName));
                }
                if (!columns.contains("XJ_ST_FLAG")) {
                    throw new RuntimeException(String.format("增量更新模式下,目标表(%s)必须包含字段类型为字符(长度最少为30)，字段名为XJ_ST_FLAG(区分大小写)的字段,建议在该字段建立位图索引", tableName));
                }
                Statement st = connection.createStatement();
                st.execute(String.format("update  %s set XJ_ST_FLAG= null", tableName));
            }

        }
    }

    public boolean getUpsert() {
        if (this.insertMode.equalsIgnoreCase("increment")) {
            if (this.getIncrementMode().equalsIgnoreCase("update")) {
                return true;
            }
        }
        return false;
    }
}
