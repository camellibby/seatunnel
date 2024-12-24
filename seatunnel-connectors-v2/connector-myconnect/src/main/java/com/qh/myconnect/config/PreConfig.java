package com.qh.myconnect.config;

import org.apache.seatunnel.api.configuration.util.OptionMark;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.qh.myconnect.dialect.JdbcDialectFactory;
import lombok.Data;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
public class PreConfig implements Serializable {
    private static final long serialVersionUID = -1L;

    @OptionMark(description = "插入模式")
    private String insertMode; // 插入模式 全量 complete  增量 increment

    @OptionMark(description = "全量模式是否清空表 true清 false不清")
    private boolean cleanTableWhenComplete;

    @OptionMark(description = "无数据输入继续清空表 true清 false不清")
    private boolean cleanTableWhenCompleteNoDataIn = false;

    @OptionMark(description = "增量模式 update或者zipper模式 ")
    private String incrementMode;

    @OptionMark(description = "增量模式 忽略时间戳比对 ")
    private Boolean ignoreTstamp = true;

    @OptionMark(description = "增量模式 忽略对比的字段 ")
    private List<String> ignoreColumns;

    @OptionMark(description = "ck 集群模式下 集群的名字")
    private String clusterName;

    public PreConfig() {
    }

    public void doPreConfig(Connection connection, JdbcSinkConfig jdbcSinkConfig)
            throws SQLException {
        String tableName = jdbcSinkConfig.getTable();
        String schemaPattern = jdbcSinkConfig.getDbSchema();
        if (Objects.equals(schemaPattern, "")) {
            schemaPattern = null;
        }

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

        if (this.insertMode.equalsIgnoreCase("complete")
            && this.cleanTableWhenComplete
            && this.cleanTableWhenCompleteNoDataIn) {
            Statement st = connection.createStatement();
            st.execute(
                    JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                            .truncateTable(jdbcSinkConfig));
            st.close();
        }

        if (this.insertMode.equalsIgnoreCase("increment")) {
            if (null == jdbcSinkConfig.getPrimaryKeys()
                || jdbcSinkConfig.getPrimaryKeys().isEmpty()) {
                throw new RuntimeException(String.format("增量更新模式下,未标示逻辑主键", tableName));
            }
            String tmpTableName = "XJ$_" + tableName;
            String copyTableOnlyColumnSql =
                    JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                            .copyTableOnlyColumn(tableName, tmpTableName, jdbcSinkConfig);
            if (clusterName != null && !clusterName.equalsIgnoreCase("")) {
                String dropSqlCluster =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTableOnCluster(
                                        jdbcSinkConfig,
                                        ((ClickHouseConnectionImpl) connection)
                                                .getCurrentDatabase(),
                                        tmpTableName,
                                        clusterName);
                copyTableOnlyColumnSql =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .copyTableOnlyColumnOnCluster(
                                        tableName,
                                        tmpTableName,
                                        jdbcSinkConfig,
                                        clusterName,
                                        ((ClickHouseConnectionImpl) connection)
                                                .getCurrentDatabase());
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSqlCluster);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println("删除报错意味着没有表");
                }
            }
            else {
                String dropSql =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTable(jdbcSinkConfig, tmpTableName);
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSql);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println(dropSql + "删除报错意味着没有表" + e.getMessage());
                }
            }
            PreparedStatement preparedStatement1 =
                    connection.prepareStatement(copyTableOnlyColumnSql);
            preparedStatement1.execute();
            preparedStatement1.close();
            if (!jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                PreparedStatement preparedStatement2 =
                        connection.prepareStatement(
                                JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                        .createIndex(tmpTableName, jdbcSinkConfig));
                preparedStatement2.execute();
                preparedStatement2.close();
            }
            if (jdbcSinkConfig.getDbType().equalsIgnoreCase("pgsql")) {
                PreparedStatement preparedStatement = connection.prepareStatement(
                        String.format("ALTER TABLE  \"%s\".\"%s\" REPLICA IDENTITY FULL", jdbcSinkConfig.getDbSchema(),
                                tmpTableName));
                preparedStatement.execute();
                preparedStatement.close();
            }
        }
    }

    public void dropUcTable(Connection connection, JdbcSinkConfig jdbcSinkConfig) throws SQLException{
        String tableName = jdbcSinkConfig.getTable();
        if (this.insertMode.equalsIgnoreCase("increment")) {
            String tmpTableName = "XJ$_" + tableName;
            String copyTableOnlyColumnSql =
                    JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                            .copyTableOnlyColumn(tableName, tmpTableName, jdbcSinkConfig);
            if (clusterName != null && !clusterName.equalsIgnoreCase("")) {
                String dropSqlCluster =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTableOnCluster(
                                        jdbcSinkConfig,
                                        ((ClickHouseConnectionImpl) connection)
                                                .getCurrentDatabase(),
                                        tmpTableName,
                                        clusterName);
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSqlCluster);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println("删除报错意味着没有表");
                }
            }
            else {
                String dropSql =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTable(jdbcSinkConfig, tmpTableName);
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSql);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println(dropSql + "删除报错意味着没有表" + e.getMessage());
                }
            }
        }
    }
}
