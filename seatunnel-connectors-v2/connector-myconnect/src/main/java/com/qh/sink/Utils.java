package com.qh.sink;

import com.mysql.cj.jdbc.Driver;
import com.qh.config.JdbcSinkConfig;
import com.qh.config.PreConfig;
import com.qh.dialect.JdbcConnectorException;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.stringtemplate.v4.ST;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class Utils {
    public static SeaTunnelRowType initTableField(Connection conn,
                                                  JdbcDialect jdbcDialect,
                                                  JdbcSinkConfig jdbcSinkConfig) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData =
                    jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                // Support AS syntax
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

    public static List<UniqueConstraint> getUniqueConstraints(Connection conn, String schema, String table) throws SQLException {
        Map<String, UniqueConstraint> constraints = new HashMap<>();

        DatabaseMetaData dm = conn.getMetaData();
        ResultSet rs = dm.getIndexInfo(null, null, table, true, true);
        while (rs.next()) {

            String indexName = rs.getString("index_name");
            String columnName = rs.getString("column_name");

            if (indexName != null) {
                UniqueConstraint constraint = new UniqueConstraint();
                constraint.table = table;
                constraint.name = indexName;
                constraint.columns.add(columnName);

                constraints.compute(indexName, (key, value) -> {
                    if (value == null) {
                        return constraint;
                    }
                    value.columns.add(columnName);
                    return value;
                });
            }
        }

        return new ArrayList<>(constraints.values());
    }

    public static SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
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

    public static String Object2String(Object value) {
        if (null != value) {
            if (value instanceof java.util.Date) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return formatter.format(((Date) value).toInstant());
            } else if (value instanceof Long) {
                return Long.toString((Long) value);
            } else if (value instanceof BigDecimal) {
                return value.toString();
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

    public static void insertLog(Long writeCount, Long modifyCount, Long deleteCount, Long keepCount, Long insertCount, String flinkJobId, LocalDateTime startTime, LocalDateTime endTime) {
        String user = System.getenv("MYSQL_MASTER_USER");
        String password = System.getenv("MYSQL_MASTER_PWD");
        String dbHost = System.getenv("MYSQL_MASTER_HOST");
        String dbPort = System.getenv("MYSQL_MASTER_PORT");
        String dbName = System.getenv("PANGU_DB");
        String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        String querySql = "select  count(1) logCount from  seatunnel_jobs_history where flinkJobId=?";
        try (Connection connection = new Driver().connect(url, info)) {
            PreparedStatement statement = connection.prepareStatement(querySql);
            statement.setString(1, flinkJobId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int logCount = resultSet.getInt("logCount");
                if (logCount == 1) {
                    String updateString = "update seatunnel_jobs_history  " +
                            "   SET writeCount  = <writeCount>,  " +
                            "       updateCount = <updateCount>,  " +
                            "       deleteCount = <deleteCount>,  " +
                            "       keepCount   = <keepCount>,  " +
                            "       insertCount = <insertCount>  " +
                            " WHERE flinkJobId = '<flinkJobId>'";
                    ST template = new ST(updateString);
                    template.add("writeCount", writeCount);
                    template.add("updateCount", modifyCount);
                    template.add("deleteCount", deleteCount);
                    template.add("keepCount", keepCount);
                    template.add("insertCount", insertCount);
                    template.add("flinkJobId", flinkJobId);
                    String updateSql = template.render();
                    PreparedStatement psUpdate = connection.prepareStatement(updateSql);
                    psUpdate.execute();
                } else {
                    String insertString = "INSERT INTO seatunnel_jobs_history  " +
                            "  (jobId,  " +
                            "   flinkJobId,  " +
                            "   jobStatus,  " +
                            "   startTime,  " +
                            "   endTime,  " +
                            "   writeCount,  " +
                            "   updateCount,  " +
                            "   deleteCount,  " +
                            "   keepCount,  " +
                            "   insertCount )  " +
                            "VALUES  " +
                            "  ('<jobId>',  " +
                            "   '<flinkJobId>',  " +
                            "   'FINISHED',  " +
                            "   '<startTime>',  " +
                            "   '<endTime>',  " +
                            "   <writeCount>,  " +
                            "   <updateCount>,  " +
                            "   <deleteCount>,  " +
                            "   <keepCount>,  " +
                            "   <insertCount>)";
                    ST template = new ST(insertString);
                    template.add("jobId", flinkJobId);
                    template.add("flinkJobId", flinkJobId);
                    template.add("writeCount", writeCount);
                    template.add("updateCount", modifyCount);
                    template.add("deleteCount", deleteCount);
                    template.add("keepCount", keepCount);
                    template.add("insertCount", insertCount);
                    template.add("startTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    template.add("endTime", endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    String insertSql = template.render();
                    PreparedStatement psInsert = connection.prepareStatement(insertSql);
                    psInsert.execute();

                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static class UniqueConstraint {
        public String table;
        public String name;
        public List<String> columns = new ArrayList<>();

        public String toString() {
            return String.format("[%s] %s: %s", table, name, columns);
        }
    }
}
