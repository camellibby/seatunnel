/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qh.sqlcdc.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;

import com.qh.sqlcdc.config.JdbcConfig;
import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SqlCdcReader implements SourceReader<SeaTunnelRow, SqlCdcSourceSplit> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SourceReader.Context context;
    private final JdbcConfig sourceJdbcConfig;
    private final List<Integer> keysIndex = new ArrayList<>();
    private final SeaTunnelRowType typeInfoOperateflagOperatetime;

    SqlCdcReader(
            SqlCdcConfig sqlCdcConfig, SourceReader.Context context, SeaTunnelRowType typeInfo) {
        this.sqlCdcConfig = sqlCdcConfig;
        this.context = context;
        this.typeInfoOperateflagOperatetime = typeInfo;
        {
            JdbcConfig jdbcConfig = new JdbcConfig();
            jdbcConfig.setUser(sqlCdcConfig.getUser());
            jdbcConfig.setPassWord(sqlCdcConfig.getPassWord());
            jdbcConfig.setUrl(sqlCdcConfig.getUrl());
            jdbcConfig.setDbType(sqlCdcConfig.getDbType());
            jdbcConfig.setQuery(sqlCdcConfig.getQuery());
            jdbcConfig.setDriver(sqlCdcConfig.getDriver());
            this.sourceJdbcConfig = jdbcConfig;
        }
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Util util = new Util();
        try (Connection conn = util.getConnection(sourceJdbcConfig)) {
            JdbcDialect jdbcDialect =
                    JdbcDialectFactory.getJdbcDialect(sourceJdbcConfig.getDbType());
            SeaTunnelRowType typeInfo =
                    getTypeInfo(conn, jdbcDialect, this.sqlCdcConfig.getQuery());
            PreparedStatement ps = conn.prepareStatement(this.sqlCdcConfig.getQuery());
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();
            SeaTunnelRow seaTunnelRow = null;
            while (resultSet.next()) {
                seaTunnelRow = jdbcDialect.getRowConverter().toInternal(resultSet, typeInfo);
                output.collect(seaTunnelRow);
            }
            // 通知下游可以做删除数据的操作了 这里只是发个标记位 不是真正的删除这行数据
            assert seaTunnelRow != null;
            seaTunnelRow.setRowKind(RowKind.DELETE);
            output.collect(seaTunnelRow);
            Thread.sleep(1000 * 3600);
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
    }

    @Override
    public List<SqlCdcSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<SqlCdcSourceSplit> splits) {}

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private SeaTunnelRowType getTypeInfo(Connection conn, JdbcDialect jdbcDialect, String sql) {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeQuery();
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
            ps.close();
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }
}
