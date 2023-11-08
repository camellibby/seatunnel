package com.qh.sink;

import com.google.auto.service.AutoService;
import com.qh.config.JdbcSinkConfig;
import com.qh.config.PreConfig;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.oracle.OracleDialect;
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
import java.time.LocalDateTime;


@AutoService(SeaTunnelSink.class)
@Slf4j
public class MySink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private SeaTunnelRowType seaTunnelRowType;
    private ReadonlyConfig config;

    private JobContext jobContext;

    private JdbcSinkConfig jdbcSinkConfig;

    @Override
    public void setJobContext(JobContext jobContext) {
        super.setJobContext(jobContext);
        this.jobContext = jobContext;
    }

    public MySink(SeaTunnelRowType seaTunnelRowType, ReadonlyConfig config, JdbcSinkConfig jdbcSinkConfig) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
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
        try (Connection conn = DriverManager.getConnection(jdbcSinkConfig.getUrl(), jdbcSinkConfig.getUser(), jdbcSinkConfig.getPassWord())) {
            JdbcDialect jdbcDialect = null;
            if (jdbcSinkConfig.getDbType().equalsIgnoreCase("ORACLE")) {
                jdbcDialect = new OracleDialect();
            }
            preConfig.doPreConfig(conn, jdbcSinkConfig.getTable(), jdbcDialect);
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
            JdbcSinkConfig jdbcSinkConfig = JdbcSinkConfig.of(config);
            PreConfig preConfig = jdbcSinkConfig.getPreConfig();
            if (preConfig.getInsertMode().equalsIgnoreCase("complete")) {
                return new MySinkWriterComplete(seaTunnelRowType, context, config, this.jobContext);
            } else {
                if (preConfig.getIncrementMode().equalsIgnoreCase("update")) {
                    return new MySinkWriterUpdate(seaTunnelRowType, context, config, this.jobContext,LocalDateTime.now());
                }
                return new MySinkWriterZipper(seaTunnelRowType, context, config, this.jobContext, LocalDateTime.now());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
