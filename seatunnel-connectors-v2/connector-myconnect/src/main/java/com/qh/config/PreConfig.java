package com.qh.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.qh.dialect.JdbcDialectFactory;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.util.OptionMark;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

@Data
public class PreConfig implements Serializable {
    private static final long serialVersionUID = -1L;
    @OptionMark(description = "插入模式")
    private String insertMode;//插入模式 全量 complete  增量 increment
    @OptionMark(description = "全量模式是否清空表 true清 false不清")
    private boolean cleanTableWhenComplete;
    @OptionMark(description = "增量模式 update或者zipper模式 ")
    private String incrementMode;

    @JsonIgnore
    private static final List<String> zipperColumns = Arrays.asList("operateFlag", "operateTime");


    public PreConfig() {
    }

    public void doPreConfig(Connection connection, JdbcSinkConfig jdbcSinkConfig) throws SQLException {


        String tableName = jdbcSinkConfig.getTable();
        String schemaPattern = jdbcSinkConfig.getDbType().equalsIgnoreCase("oracle") ?
                jdbcSinkConfig.getUser().toUpperCase() :
                jdbcSinkConfig.getDatabase();

        DatabaseMetaData metadata = connection.getMetaData();
        ResultSet rsColumn = metadata.getColumns(null, schemaPattern, tableName, null);
        List<String> columns = new ArrayList<>();
        while (rsColumn.next()) {
            String name = rsColumn.getString("COLUMN_NAME");
            columns.add(name);
        }

        if (columns.isEmpty()) {
            throw new RuntimeException("目标表不存在");
        }


        if (this.insertMode.equalsIgnoreCase("complete")) {
            if (this.cleanTableWhenComplete) {
                Statement st = connection.createStatement();
                st.execute(String.format("truncate  table %s", tableName));
                st.close();
            }
        }

        if (this.insertMode.equalsIgnoreCase("increment")) {
            if (null == jdbcSinkConfig.getPrimaryKeys() || jdbcSinkConfig.getPrimaryKeys().isEmpty()) {
                throw new RuntimeException(String.format("增量更新模式下,未标示逻辑主键", tableName));
            }


            String tmpTableName = "UC_" + tableName;
            try {
                PreparedStatement drop = connection.prepareStatement(String.format("drop table  %s", tmpTableName));
                drop.execute();
                drop.close();
            } catch (SQLException e) {
                System.out.println("删除报错意味着没有表");
            }

            String copyTableOnlyColumnSql = JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType()).copyTableOnlyColumn(tableName, tmpTableName, jdbcSinkConfig.getPrimaryKeys());
            PreparedStatement preparedStatement1 = connection.prepareStatement(copyTableOnlyColumnSql);
            preparedStatement1.execute();
            preparedStatement1.close();
            if (!jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                PreparedStatement preparedStatement2 = connection.prepareStatement(String.format(
                        "CREATE UNIQUE INDEX %s ON %s(%s)",
                        tmpTableName,
                        tmpTableName,
                        StringUtils.join(jdbcSinkConfig.getPrimaryKeys(), ',')
                ));
                preparedStatement2.execute();
                preparedStatement2.close();
            }

        }
    }
}
