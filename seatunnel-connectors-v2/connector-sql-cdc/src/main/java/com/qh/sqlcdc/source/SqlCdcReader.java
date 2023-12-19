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

import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.io.IOException;
import java.sql.*;

@Slf4j
public class SqlCdcReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SingleSplitReaderContext context;

    private Connection conn;

    private final JdbcDialect jdbcDialect;
    private final SeaTunnelRowType typeInfo;

    SqlCdcReader(SqlCdcConfig sqlCdcConfig, SingleSplitReaderContext context, JdbcDialect jdbcDialect, SeaTunnelRowType typeInfo) {
        this.conn = new Util().getConnection(sqlCdcConfig);
        this.sqlCdcConfig = sqlCdcConfig;
        this.context = context;
        this.jdbcDialect = jdbcDialect;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws IOException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            String sql = this.sqlCdcConfig.getQuery();
            while (true) {
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.executeQuery();
                ResultSet resultSet = ps.getResultSet();
                while (resultSet.next()) {
                    SeaTunnelRow seaTunnelRow = jdbcDialect.getRowConverter().toInternal(resultSet, typeInfo);
                    seaTunnelRow.setRowKind(RowKind.UPDATE_AFTER);
                    output.collect(seaTunnelRow);
                }
                ps.close();
                log.info("下一轮循环开始");
                Thread.sleep(2 * 1000);
            }
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
    }
}
