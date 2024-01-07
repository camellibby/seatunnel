/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * InputFormat to read data from a database and generate Rows. The InputFormat has to be configured
 * using the supplied InputFormatBuilder. A valid RowTypeInfo must be properly configured in the
 * builder
 */
public class JdbcInputFormat implements Serializable {

    private static final long serialVersionUID = 2L;
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);

    protected JdbcConnectionProvider connectionProvider;
    protected JdbcRowConverter jdbcRowConverter;
    protected String queryTemplate;
    protected SeaTunnelRowType typeInfo;
    protected int fetchSize;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    protected Boolean autoCommit;

    protected transient PreparedStatement statement;
    protected transient ResultSet resultSet;

    protected boolean hasNext;

    protected JdbcDialect jdbcDialect;


    private JdbcSourceConfig jdbcSourceConfig;

    public JdbcInputFormat(
            JdbcConnectionProvider connectionProvider,
            JdbcDialect jdbcDialect,
            SeaTunnelRowType typeInfo,
            String queryTemplate,
            int fetchSize,
            Boolean autoCommit,
            JdbcSourceConfig jdbcSourceConfig
    ) {
        this.connectionProvider = connectionProvider;
        this.jdbcRowConverter = jdbcDialect.getRowConverter();
        this.typeInfo = typeInfo;
        this.queryTemplate = queryTemplate;
        this.fetchSize = fetchSize;
        this.autoCommit = autoCommit;
        this.jdbcDialect = jdbcDialect;
        this.jdbcSourceConfig = jdbcSourceConfig;
    }

    public void openInputFormat() {
        // called once per inputFormat (on open)
        try {
            Connection dbConn = connectionProvider.getOrEstablishConnection();

            // set autoCommit mode only if it was explicitly configured.
            // keep connection default otherwise.
            if (autoCommit != null) {
                dbConn.setAutoCommit(autoCommit);
            }

            statement = jdbcDialect.creatPreparedStatement(dbConn, queryTemplate, fetchSize);
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "open() failed." + se.getMessage(),
                    se);
        } catch (ClassNotFoundException cnfe) {
            throw new JdbcConnectorException(
                    CommonErrorCode.CLASS_NOT_FOUND,
                    "JDBC-Class not found. - " + cnfe.getMessage(),
                    cnfe);
        }
    }

    public void closeInputFormat() {
        // called once per inputFormat (on close)
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
        } finally {
            statement = null;
        }

        connectionProvider.closeConnection();
    }

    /**
     * Connects to the source database and executes the query
     *
     * @param inputSplit which is ignored if this InputFormat is executed as a non-parallel source,
     *                   a "hook" to the query parameters otherwise (using its <i>parameterId</i>)
     * @throws IOException if there's an error during the execution of the query
     */
    public void open(JdbcSourceSplit inputSplit) throws IOException {
        try {
            if (!connectionProvider.isConnectionValid()) {
                openInputFormat();
            }
            Object[] parameterValues = inputSplit.getParameterValues();
            List<Object> parameterValuesNew = new ArrayList<>();
            try {
                Connection dbConn = connectionProvider.getOrEstablishConnection();
                if (parameterValues != null) {
                    ResultSet splitValue = this.jdbcDialect.getSplitValue(dbConn, this.jdbcSourceConfig, parameterValues);
                    while (splitValue.next()) {
                        Object object = splitValue.getObject(1);
                        parameterValuesNew.add(object);
                    }
                    if (parameterValuesNew.size() == 1) {
                        parameterValuesNew.add(parameterValuesNew.get(0));
                    }
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (!parameterValuesNew.isEmpty()) {
                if (parameterValuesNew.get(0) == null && parameterValuesNew.get(1) == null) {
                    try {
                        Connection dbConn = connectionProvider.getOrEstablishConnection();
                        this.queryTemplate = String.format("select  * from  (%s) a where %s is null ",
                                this.jdbcSourceConfig.getQuery(),
                                this.jdbcSourceConfig.getPartitionColumn().get()
                        );
                        this.statement = jdbcDialect.creatPreparedStatement(dbConn, queryTemplate, fetchSize);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (parameterValuesNew.get(0) != null && parameterValuesNew.get(1) == null) {
                    try {
                        Connection dbConn = connectionProvider.getOrEstablishConnection();
                        this.queryTemplate = String.format("select  * from  (%s) a where %s>=? or  %s is null ",
                                this.jdbcSourceConfig.getQuery(),
                                this.jdbcSourceConfig.getPartitionColumn().get(),
                                this.jdbcSourceConfig.getPartitionColumn().get()
                        );
                        this.statement = jdbcDialect.creatPreparedStatement(dbConn, queryTemplate, fetchSize);
                        Object param = parameterValuesNew.get(0);
                        if (param instanceof String) {
                            statement.setString(1, (String) param);
                        } else if (param instanceof Long) {
                            statement.setLong(1, (Long) param);
                        } else if (param instanceof Integer) {
                            statement.setInt(1, (Integer) param);
                        } else if (param instanceof Double) {
                            statement.setDouble(1, (Double) param);
                        } else if (param instanceof Boolean) {
                            statement.setBoolean(1, (Boolean) param);
                        } else if (param instanceof Float) {
                            statement.setFloat(1, (Float) param);
                        } else if (param instanceof BigDecimal) {
                            statement.setBigDecimal(1, (BigDecimal) param);
                        } else if (param instanceof Byte) {
                            statement.setByte(1, (Byte) param);
                        } else if (param instanceof Short) {
                            statement.setShort(1, (Short) param);
                        } else if (param instanceof Date) {
                            statement.setDate(1, (Date) param);
                        } else if (param instanceof Time) {
                            statement.setTime(1, (Time) param);
                        } else if (param instanceof Timestamp) {
                            statement.setTimestamp(1, (Timestamp) param);
                        } else if (param instanceof Array) {
                            statement.setArray(1, (Array) param);
                        } else {
                            // extends with other types if needed
                            throw new JdbcConnectorException(
                                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                    "open() failed. Parameter "
                                            + 0
                                            + " of type "
                                            + param.getClass()
                                            + " is not handled (yet).");
                        }
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (parameterValuesNew.get(0) != null && parameterValuesNew.get(1) != null) {
                    try {
                        Connection dbConn = connectionProvider.getOrEstablishConnection();
                        this.queryTemplate = String.format("select  * from  (%s) a where %s>=? and   %s <=? ",
                                this.jdbcSourceConfig.getQuery(),
                                this.jdbcSourceConfig.getPartitionColumn().get(),
                                this.jdbcSourceConfig.getPartitionColumn().get()
                        );
                        this.statement = jdbcDialect.creatPreparedStatement(dbConn, queryTemplate, fetchSize);
                        for (int i = 0; i < parameterValuesNew.size(); i++) {
                            Object param = parameterValuesNew.get(i);
                            if (param instanceof String) {
                                statement.setString(i + 1, (String) param);
                            } else if (param instanceof Long) {
                                statement.setLong(i + 1, (Long) param);
                            } else if (param instanceof Integer) {
                                statement.setInt(i + 1, (Integer) param);
                            } else if (param instanceof Double) {
                                statement.setDouble(i + 1, (Double) param);
                            } else if (param instanceof Boolean) {
                                statement.setBoolean(i + 1, (Boolean) param);
                            } else if (param instanceof Float) {
                                statement.setFloat(i + 1, (Float) param);
                            } else if (param instanceof BigDecimal) {
                                statement.setBigDecimal(i + 1, (BigDecimal) param);
                            } else if (param instanceof Byte) {
                                statement.setByte(i + 1, (Byte) param);
                            } else if (param instanceof Short) {
                                statement.setShort(i + 1, (Short) param);
                            } else if (param instanceof Date) {
                                statement.setDate(i + 1, (Date) param);
                            } else if (param instanceof Time) {
                                statement.setTime(i + 1, (Time) param);
                            } else if (param instanceof Timestamp) {
                                statement.setTimestamp(i + 1, (Timestamp) param);
                            } else if (param instanceof Array) {
                                statement.setArray(i + 1, (Array) param);
                            } else {
                                // extends with other types if needed
                                throw new JdbcConnectorException(
                                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                        "open() failed. Parameter "
                                                + i
                                                + " of type "
                                                + param.getClass()
                                                + " is not handled (yet).");
                            }
                        }
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                }
            } else {
                try {
                    Connection dbConn = connectionProvider.getOrEstablishConnection();
                    this.queryTemplate = String.format("select  * from  (%s) a  ",
                            this.jdbcSourceConfig.getQuery()
                    );
                    this.statement = jdbcDialect.creatPreparedStatement(dbConn, queryTemplate, fetchSize);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED, "open() failed." + se.getMessage(), se);
        }
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    public void close() throws IOException {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (SQLException se) {
            LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    public boolean reachedEnd() {
        return !hasNext;
    }

    /**
     * Convert a row of data to seatunnelRow
     */
    public SeaTunnelRow nextRecord() {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = jdbcRowConverter.toInternal(resultSet, typeInfo);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED,
                    "Couldn't read data - " + se.getMessage(),
                    se);
        } catch (NullPointerException npe) {
            throw new JdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED, "Couldn't access resultSet", npe);
        }
    }
}
