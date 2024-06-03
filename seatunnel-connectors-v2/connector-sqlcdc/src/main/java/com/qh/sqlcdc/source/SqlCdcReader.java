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

import com.google.common.collect.Sets;
import org.apache.seatunnel.api.source.Collector;

import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.MapDifference;

@Slf4j
public class SqlCdcReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private List<SeaTunnelRow> localSeaTunnelRowsCache = new ArrayList<>();

    SqlCdcReader(
            SqlCdcConfig sqlCdcConfig, SeaTunnelRowType seaTunnelRowType) {
        this.sqlCdcConfig = sqlCdcConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Util util = new Util();
        List<SeaTunnelRow> nowSeaTunnelRows = new ArrayList<>();
        try (Connection conn = util.getConnection(this.sqlCdcConfig)) {
            JdbcDialect jdbcDialect =
                    JdbcDialectFactory.getJdbcDialect(this.sqlCdcConfig.getDbType());
            PreparedStatement ps = conn.prepareStatement(this.sqlCdcConfig.getQuery());
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();
            while (resultSet.next()) {
                SeaTunnelRow seaTunnelRow = jdbcDialect.getRowConverter().toInternal(resultSet, this.seaTunnelRowType);
                nowSeaTunnelRows.add(seaTunnelRow);
            }
            Sets.SetView<SeaTunnelRow> difference = Sets.difference(Sets.newHashSet(nowSeaTunnelRows), Sets.newHashSet(localSeaTunnelRowsCache));
            for (SeaTunnelRow seaTunnelRow : difference) {
                output.collect(seaTunnelRow);
            }
            localSeaTunnelRowsCache.clear();
            for (SeaTunnelRow nowSeaTunnelRow : nowSeaTunnelRows) {
                localSeaTunnelRowsCache.add(nowSeaTunnelRow.copy());
            }
            Thread.sleep(1000 * 3);
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
    }

}
