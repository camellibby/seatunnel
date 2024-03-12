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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.SeaTunnelDataValueIndex;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class JdbcSinkWriter implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> {

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>
            outputFormat;
    private final SinkWriter.Context context;
    private final JdbcConnectionProvider connectionProvider;
    private transient boolean isOpen;
    private List<SeaTunnelDataValueIndex> seaTunnelDataValueIndices;

    private SeaTunnelRowType rowType;

    public JdbcSinkWriter(
            SinkWriter.Context context,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            SeaTunnelRowType rowType,
            List<SeaTunnelDataValueIndex> seaTunnelDataValueIndices) {
        this.context = context;
        this.seaTunnelDataValueIndices = seaTunnelDataValueIndices;
        String[] fieldNames = new String[seaTunnelDataValueIndices.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[seaTunnelDataValueIndices.size()];
        for (int i = 0; i < seaTunnelDataValueIndices.size(); i++) {
            SeaTunnelDataValueIndex seaTunnelDataValueIndex = seaTunnelDataValueIndices.get(i);
            fieldNames[i] = seaTunnelDataValueIndex.getNewColumnName();
            fieldTypes[i] = seaTunnelDataValueIndex.getSeaTunnelDataType();
        }
        this.rowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        this.connectionProvider =
                new SimpleJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, this.rowType)
                        .build();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        Object[] fields = element.getFields();
        Object[] newfields = new Object[seaTunnelDataValueIndices.size()];
        for (int i = 0; i < this.rowType.getTotalFields(); i++) {
            String fieldName = this.rowType.getFieldName(i);
            int finalI = i;
            this.seaTunnelDataValueIndices.stream().filter(x -> x.getNewColumnName().equalsIgnoreCase(fieldName)).findFirst()
                    .ifPresent(x -> {
                        Object field = fields[x.getOriginColumnValueIndex()];
                        newfields[finalI] = field;
                    });
        }
        SeaTunnelRow newRow = new SeaTunnelRow(newfields);
        newRow.setRowKind(element.getRowKind());
        newRow.setTableId(element.getTableId());
        outputFormat.writeRecord(newRow);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        tryOpen();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED, "unable to close JDBC sink write", e);
        }
        outputFormat.close();
    }
}
