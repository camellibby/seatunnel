package com.qh.config;

import com.qh.converter.ColumnMapper;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.oracle.OracleDialect;
import com.qh.sink.Utils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.util.OptionMark;
import redis.clients.jedis.Jedis;
import scala.Predef;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

import static com.qh.sink.Utils.getUniqueConstraints;


@Data
public class PreConfig implements Serializable {
    private static final long serialVersionUID = -1L;
    @OptionMark(description = "插入模式")
    private String insertMode;//插入模式 全量 complete  增量 increment
    @OptionMark(description = "全量模式是否清空表 true清 false不清")
    private boolean cleanTableWhenComplete;
    @OptionMark(description = "增量模式 update或者zipper模式 ")
    private String incrementMode;
    private static final List<String> zipperColumns = Arrays.asList("ZIPPERFLAG", "ZIPPERSTARTTIME", "ZIPPERENDTIME");

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

    public void doPreConfig(Connection connection, String tableName, String schemaPattern) throws SQLException {
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
        ResultSet rsColumn = metadata.getColumns(null, schemaPattern, tableName, null);
        List<String> columns = new ArrayList<>();
        while (rsColumn.next()) {
            String name = rsColumn.getString("COLUMN_NAME");
            columns.add(name);
            String type = rsColumn.getString("TYPE_NAME");
            int size = rsColumn.getInt("COLUMN_SIZE");
        }

        if(columns.isEmpty()){
            throw new RuntimeException("目标表不存在");
        }

        List<Utils.UniqueConstraint> uniqueConstraints = getUniqueConstraints(connection, null, tableName);


        if (this.insertMode.equalsIgnoreCase("complete")) {
            if (this.cleanTableWhenComplete) {
                Statement st = connection.createStatement();
                st.execute(String.format("truncate  table %s", tableName));
            }
        }

        if (this.insertMode.equalsIgnoreCase("increment")) {
            if (this.incrementMode.equalsIgnoreCase("update")) {
                if (uniqueConstraints.isEmpty()) {
                    throw new RuntimeException(String.format("增量更新模式下,目标表(%s)必须包含唯一索引", tableName));
                }
            }

            if (this.incrementMode.equalsIgnoreCase("zipper")) {
                if (!uniqueConstraints.isEmpty()) {
                    uniqueConstraints.forEach(x -> {
                        if (!new HashSet<>(x.columns).containsAll(zipperColumns) || (x.columns.size() <= 3)) {
                            throw new RuntimeException(String.format("增量拉链模式下,目标表(%s)必须包含唯一索引,且唯一索引必须包含 ZIPPERFLAG,ZIPPERSTARTTIME,ZIPPERENDTIME ", tableName));
                        }
                    });
                } else {
                    throw new RuntimeException(String.format("增量拉链模式下,目标表(%s)必须包含唯一索引,且唯一索引必须包含 ZIPPERFLAG,ZIPPERSTARTTIME,ZIPPERENDTIME ", tableName));
                }
            }

            int tableCount = 0;
            String tmpTableName = "UC_" + tableName;
            String countTable = String.format("select count(1) sl from user_tables a where a.TABLE_NAME='%s'", tmpTableName);
            PreparedStatement preparedStatement = connection.prepareStatement(countTable);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                tableCount = resultSet.getInt("sl");
            }
            if (tableCount == 1) {
                PreparedStatement preparedStatement1 = connection.prepareStatement(String.format("drop table %s", tmpTableName));
                preparedStatement1.execute();
            }

            for (Utils.UniqueConstraint uniqueConstraint : uniqueConstraints) {
                PreparedStatement preparedStatement1 = connection.prepareStatement(
                        String.format("create  table %s as select  %s from %s where 1=2",
                                tmpTableName,
                                StringUtils.join(uniqueConstraint.columns, ','),
                                tableName
                        ));
                preparedStatement1.execute();
                PreparedStatement preparedStatement2 = connection.prepareStatement(String.format(
                        "CREATE UNIQUE INDEX %s ON %s(%s)",
                        tmpTableName,
                        tmpTableName,
                        StringUtils.join(uniqueConstraint.columns, ',')
                ));
                preparedStatement2.execute();
            }

        }
    }
}
