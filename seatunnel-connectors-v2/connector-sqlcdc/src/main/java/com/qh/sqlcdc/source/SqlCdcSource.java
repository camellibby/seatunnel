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

import com.alibaba.fastjson2.JSONObject;
import com.google.auto.service.AutoService;
import com.qh.sqlcdc.config.ColumnMapper;
import com.qh.sqlcdc.config.JdbcConfig;
import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

@AutoService(SeaTunnelSource.class)
@Slf4j
public class SqlCdcSource implements SeaTunnelSource<SeaTunnelRow, SqlCdcSourceSplit, ArrayList<SqlCdcSourceSplit>>,
        SupportParallelism,
        SupportColumnProjection {
    private SqlCdcConfig sqlCdcConfig;
    private JobContext jobContext;
    private JdbcDialect jdbcDialect;
    private SeaTunnelRowType typeInfo;
    private List<ColumnMapper> columnMappers;


    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "SqlCdc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.sqlCdcConfig = new SqlCdcConfig(pluginConfig);
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(sqlCdcConfig.getDbType());
        this.columnMappers = initColumnMapper();
        this.typeInfo = initTableField(this.columnMappers);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, SqlCdcSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new SqlCdcReader(this.sqlCdcConfig, readerContext, columnMappers, this.typeInfo);
    }

    @Override
    public SourceSplitEnumerator<SqlCdcSourceSplit, ArrayList<SqlCdcSourceSplit>> createEnumerator(SourceSplitEnumerator.Context<SqlCdcSourceSplit> enumeratorContext) throws Exception {
        return new SqlCdcSourceSplitEnumerator(
                enumeratorContext, sqlCdcConfig);
    }

    @Override
    public SourceSplitEnumerator<SqlCdcSourceSplit, ArrayList<SqlCdcSourceSplit>> restoreEnumerator(SourceSplitEnumerator.Context<SqlCdcSourceSplit> enumeratorContext, ArrayList<SqlCdcSourceSplit> checkpointState) throws Exception {
        return null;
    }

    private SeaTunnelRowType initTableField(List<ColumnMapper> columnMappers) {
        Util util = new Util();
        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setUser(this.sqlCdcConfig.getUser());
        jdbcConfig.setPassWord(this.sqlCdcConfig.getPassWord());
        jdbcConfig.setUrl(this.sqlCdcConfig.getUrl());
        jdbcConfig.setDbType(this.sqlCdcConfig.getDbType());
        jdbcConfig.setQuery(this.sqlCdcConfig.getQuery());
        jdbcConfig.setDriver(this.sqlCdcConfig.getDriver());
        Connection conn = util.getConnection(jdbcConfig);
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            List<String> sourceColumns = new ArrayList<>();
            columnMappers.forEach(x -> sourceColumns.add(x.getSourceColumnName()));
            String sql = String.format("select  %s from (%s) a ", StringUtils.join(sourceColumns, ","), sqlCdcConfig.getQuery());
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeQuery();
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
            if (this.sqlCdcConfig.getRecordOperation()) {
                fieldNames.add("OPERATEFLAG");
                seaTunnelDataTypes.add(BasicType.STRING_TYPE);
                fieldNames.add("OPERATETIME");
                seaTunnelDataTypes.add(BasicType.STRING_TYPE);
            }
            ps.close();
            conn.close();
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private List<ColumnMapper> initColumnMapper() {
        List<ColumnMapper> columnMappers = new ArrayList<>();
        JSONObject fieldMapper = this.sqlCdcConfig.getDirectSinkConfig().getJSONObject("field_mapper");
        fieldMapper.forEach((k, v) -> {
            ColumnMapper columnMapper = new ColumnMapper();
            columnMapper.setSourceColumnName(k);
            columnMapper.setSinkColumnName(v.toString());
            columnMappers.add(columnMapper);
        });
        columnMappers.forEach(x -> {
            this.sqlCdcConfig.getPrimaryKeys().forEach(y -> {
                if (x.getSourceColumnName().equalsIgnoreCase(y)) {
                    x.setPrimaryKey(true);
                }
            });
        });
        return columnMappers;
    }
}
