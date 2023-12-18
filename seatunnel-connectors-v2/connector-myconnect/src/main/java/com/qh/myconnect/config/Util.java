package com.qh.myconnect.config;

import com.alibaba.fastjson.JSONObject;
import com.qh.myconnect.dialect.JdbcConnectorErrorCode;
import com.qh.myconnect.dialect.JdbcConnectorException;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class Util {
    public SeaTunnelRowType initTableField(Connection conn,
                                           JdbcDialect jdbcDialect,
                                           JdbcSinkConfig jdbcSinkConfig) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData =
                    jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = rs.getString(resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getBoolean(resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getByte(resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getShort(resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getInt(resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getLong(resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getFloat(resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getDouble(resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getBigDecimal(resultSetIndex);
                    break;
                case DATE:
                    java.sql.Date sqlDate = rs.getDate(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    public String Object2String(Object value) {
        if (null != value) {
            if (value instanceof java.util.Date) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return formatter.format(((Date) value).toInstant());
            } else if (value instanceof Long) {
                return Long.toString((Long) value);
            } else if (value instanceof BigDecimal) {
                return ((BigDecimal) value).stripTrailingZeros().toPlainString();
            } else if (value instanceof LocalDate) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return ((LocalDate) value).format(df);
            } else if (value instanceof LocalDateTime) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return df.format((LocalDateTime) value);
            } else {
                return String.valueOf(value);
            }
        }
        return null;

    }

    public void insertLog(Long writeCount, Long modifyCount, Long deleteCount, Long keepCount, Long insertCount, String flinkJobId, LocalDateTime startTime, LocalDateTime endTime) throws Exception {
        JSONObject param = new JSONObject();
        param.put("flinkJobId", flinkJobId);
        param.put("writeCount", writeCount);
        param.put("modifyCount", modifyCount);
        param.put("deleteCount", deleteCount);
        param.put("keepCount", keepCount);
        param.put("insertCount", insertCount);
        param.put("startTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        param.put("endTime", endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String st_log_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/gatherJobLog";
        this.sendPostRequest(st_log_url, param.toString());

    }


    public java.sql.Driver loadDriver(String driverName) throws ClassNotFoundException {
        checkNotNull(driverName);
        Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            java.sql.Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (java.sql.Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CREATE_DRIVER_FAILED,
                    "Fail to create driver of class " + driverName,
                    ex);
        }
    }

    public void sendPostRequest(String url, String data) throws Exception {
        URL apiUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);

        OutputStream outputStream = connection.getOutputStream();
        outputStream.write(data.getBytes());
        outputStream.flush();
        outputStream.close();

        // 处理响应
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            // 成功处理响应
        } else {
            // 处理失败响应
        }
        connection.disconnect();
    }

    public Connection getConnection(JdbcSinkConfig jdbcSinkConfig) {
        try {
            Driver driver = this.loadDriver(jdbcSinkConfig.getDriver());
            Properties info = new Properties();
            if (jdbcSinkConfig.getUser() != null) {
                info.setProperty("user", jdbcSinkConfig.getUser());
            }
            if (jdbcSinkConfig.getPassWord() != null) {
                info.setProperty("password", jdbcSinkConfig.getPassWord());
            }
            Connection conn = driver.connect(jdbcSinkConfig.getUrl(), info);
            if (conn == null) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                        "No suitable driver found for " + jdbcSinkConfig.getUrl());
            }
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
