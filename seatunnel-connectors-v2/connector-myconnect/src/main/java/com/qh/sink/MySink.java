package com.qh.sink;

import com.google.auto.service.AutoService;
import com.qh.config.JdbcSinkConfig;
import com.qh.config.PreConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;


import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


@AutoService(SeaTunnelSink.class)
@Slf4j
public class MySink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private SeaTunnelRowType seaTunnelRowType;
    private ReadonlyConfig config;

    private JobContext jobContext;

    @Override
    public void setJobContext(JobContext jobContext) {
        super.setJobContext(jobContext);
        this.jobContext=jobContext;
    }

    private List<String> zipperColumns = Arrays.asList("zipperFlag", "zipperTime");

    public MySink(SeaTunnelRowType seaTunnelRowType, ReadonlyConfig config) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.config = config;
    }

    public MySink() {

    }

    @Override
    public String getPluginName() {
        return "XjJdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
        JdbcSinkConfig jdbcSinkConfig = JdbcSinkConfig.of(config);
        PreConfig preConfig = jdbcSinkConfig.getPreConfig();
        String insertMode = preConfig.getInsertMode();

        Connection connection = null;
        String userName = jdbcSinkConfig.getUser();
        String password = jdbcSinkConfig.getPassWord();
        String url = jdbcSinkConfig.getUrl();
        Properties info = new Properties();
        info.setProperty("user", userName);
        info.setProperty("password", password);

        if (jdbcSinkConfig.getDbType().equalsIgnoreCase("mysql")) {
            try {
                connection = new com.mysql.cj.jdbc.Driver().connect(url, info);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        if (jdbcSinkConfig.getDbType().equalsIgnoreCase("oracle")) {
            try {
                connection = new oracle.jdbc.driver.OracleDriver().connect(url, info);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }


        //全量模式 判断是否需要清空表
        if (insertMode.equalsIgnoreCase("complete")) {
            boolean cleanTableWhenComplete = preConfig.isCleanTableWhenComplete();
            if (cleanTableWhenComplete) {
                log.info("-------------------------------开始清空表-----------------------------------");
                try {
                    Statement stat = connection.createStatement();
                    String truncateSql = "truncate table " + jdbcSinkConfig.getTable();
                    stat.execute(truncateSql);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        //增量模式 检查表是否有主键
        if (insertMode.equalsIgnoreCase("increment")) {
            try {
                DatabaseMetaData meta = connection.getMetaData();
                List<String> allColumns = new ArrayList<>();
                boolean havePrimaryKeys = false;
                try (ResultSet primaryKeys = meta.getPrimaryKeys(null, null, jdbcSinkConfig.getTable());
                     ResultSet columns = meta.getColumns(null, null, jdbcSinkConfig.getTable(), "%");) {
                    while (primaryKeys.next()) {
                        havePrimaryKeys = true;
                    }
                    while (columns.next()) {
                        allColumns.add(columns.getString("COLUMN_NAME"));
                    }
                }
                if (!havePrimaryKeys) {
                    throw new RuntimeException("目标表不存在主键,增量模式要求目标表必须存在物理主键");
                }
                // 增量拉链表必须包含 "zipperFlag", "zipperTime" 2个字段
                if (preConfig.getIncrementMode() != null && preConfig.getIncrementMode().equalsIgnoreCase("zipper")) {
                    if (!allColumns.containsAll(zipperColumns)) {
                        throw new RuntimeException("增量模式(拉链选项)要求目标表必须包含 zipperFlag与zipperTime字段，且段类型为字符");
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        try {
            return new MySinkWriter(seaTunnelRowType, context, config,this.jobContext);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
