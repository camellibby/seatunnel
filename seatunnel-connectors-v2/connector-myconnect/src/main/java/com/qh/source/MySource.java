package com.qh.source;


import com.google.auto.service.AutoService;
import com.qh.config.MySourceOptions;
import com.qh.dialect.JdbcDialectTypeMapper;
import com.qh.dialect.mysql.MySqlTypeMapper;
import com.qh.dialect.oracle.OracleTypeMapper;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

@AutoService(SeaTunnelSource.class)
public class MySource implements SeaTunnelSource<SeaTunnelRow, MySourceSplit, MySourceState>,
        SupportParallelism,
        SupportColumnProjection {

    private SeaTunnelRowType typeInfo;
    private List<MySourceOptions.DbConfig> dbConfigs;

    @Override
    public String getPluginName() {
        return "XjSource";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        this.dbConfigs = config.get(MySourceOptions.DB_CONFIGS);
        MySourceOptions.DbConfig dbConfig = dbConfigs.get(0);
        this.typeInfo = initSeaTunnelRowType(dbConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, MySourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<MySourceSplit, MySourceState> createEnumerator(SourceSplitEnumerator.Context<MySourceSplit> enumeratorContext) throws Exception {
        return new MySourceSplitEnumerator(enumeratorContext, dbConfigs);
    }

    @Override
    public SourceSplitEnumerator<MySourceSplit, MySourceState> restoreEnumerator(SourceSplitEnumerator.Context<MySourceSplit> enumeratorContext, MySourceState checkpointState) throws Exception {
        return new MySourceSplitEnumerator(enumeratorContext, dbConfigs, checkpointState.getAssignedSplits());
    }

    public SeaTunnelRowType initSeaTunnelRowType(MySourceOptions.DbConfig dbConfig) {
        String sql = null;
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        JdbcDialectTypeMapper jdbcDialectTypeMapper = null;
        if (dbConfig.getUrl().startsWith("jdbc:mysql")) {
            jdbcDialectTypeMapper = new MySqlTypeMapper();
            sql = String.format("select  * from (%s) a limit 1", dbConfig.getQuery());
        }
        if (dbConfig.getUrl().startsWith("jdbc:oracle")) {
            jdbcDialectTypeMapper = new OracleTypeMapper();
            sql = String.format(" select * from (%s) where rownum=1", dbConfig.getQuery());
        }
        try {
            Class.forName(dbConfig.getDriver());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (Connection connection = DriverManager.getConnection(dbConfig.getUrl(), dbConfig.getUserName(), dbConfig.getPassWord());
             PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
            try (ResultSet resultSet = preparedStatement.executeQuery();) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    // Support AS syntax
                    fieldNames.add(resultSetMetaData.getColumnLabel(i));
                    seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }
}
