package com.qh.sink;

import com.qh.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MySinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final AtomicLong rowCounter = new AtomicLong(0);
    private final SinkWriter.Context context;

    private final List<Integer> primaryKeysIndex = new ArrayList<>();

//    private ReadonlyConfig config;

    private String delSql = "delete from ";
    private String insertSql = "insert into ";

    private Integer commitSize = 0;
    private List<String> primaryKeysValues = new ArrayList<>();
    private List<String> allColumnValus = new ArrayList<>();

    private final JdbcSinkConfig jdbcSinkConfig;

    private Connection connection;
    private DSLContext dslContext;

    public MySinkWriter(SeaTunnelRowType seaTunnelRowType, SinkWriter.Context context, ReadonlyConfig config) throws SQLException {
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
//        this.config = config;
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        log.info("output rowType: {}", fieldsInfo(seaTunnelRowType));
        List<String> primaryKeys = this.jdbcSinkConfig.getPrimaryKeys();
        for (String column : primaryKeys) {
            for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                if (column.equalsIgnoreCase(seaTunnelRowType.getFieldName(i))) {
                    primaryKeysIndex.add(i);
                    break;
                }
            }
        }

        String userName = this.jdbcSinkConfig.getUser();
        String password = this.jdbcSinkConfig.getPassWord();
        String url = this.jdbcSinkConfig.getUrl();
        try {
            this.connection = DriverManager.getConnection(url, userName, password);
            this.dslContext = DSL.using(connection, SQLDialect.MYSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        this.commitSize++;
        Object[] fields = element.getFields();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < fieldTypes.length; i++) {
            arr[i] = fieldToString(fieldTypes[i], fields[i]);
        }
        List<String> value = new ArrayList<>();
        for (Integer columnIndex : primaryKeysIndex) {
            value.add(arr[columnIndex]);
        }

        this.primaryKeysValues.add(String.format("(%s)", StringUtils.join(value, ",")));
        this.allColumnValus.add(String.format("(%s)", StringUtils.join(arr, ",")));
        if (this.commitSize > 100) {
            this.delSql += String.format(this.jdbcSinkConfig.getTable() + " where (%s) in ", StringUtils.join(this.jdbcSinkConfig.getPrimaryKeys(), ","))
                    + String.format("(%s)", StringUtils.join(this.primaryKeysValues, ","));
            this.insertSql += String.format(this.jdbcSinkConfig.getTable() + "(%s)", StringUtils.join(seaTunnelRowType.getFieldNames(), ",")) +
                    String.format("values %s", StringUtils.join(this.allColumnValus, ","));
            log.info(delSql);
            log.info(insertSql);
            this.batchCommit();
            this.commitSize = 0;
            this.delSql = "delete from ";
            this.insertSql = "insert into ";
            this.primaryKeysValues.clear();
            this.allColumnValus.clear();

        }
        log.info(
                "subtaskIndex={}  rowIndex={}:  SeaTunnelRow#tableId={} SeaTunnelRow#kind={} : {}",
                context.getIndexOfSubtask(),
                rowCounter.incrementAndGet(),
                element.getTableId(),
                element.getRowKind(),
                StringUtils.join(arr, ", "));
    }

    @Override
    public void close() {
        this.delSql += String.format(this.jdbcSinkConfig.getTable() + " where (%s) in ", StringUtils.join(this.jdbcSinkConfig.getPrimaryKeys(), ","))
                + String.format("(%s)", StringUtils.join(primaryKeysValues, ","));
        this.insertSql += String.format(this.jdbcSinkConfig.getTable() + "(%s)", StringUtils.join(seaTunnelRowType.getFieldNames(), ",")) +
                String.format("values %s", StringUtils.join(this.allColumnValus, ","));
        this.batchCommit();
        this.commitSize = 0;
        this.delSql = "delete from ";
        this.insertSql = "insert into ";
        this.primaryKeysValues.clear();
        this.allColumnValus.clear();
        try {
            this.connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    private String fieldsInfo(SeaTunnelRowType seaTunnelRowType) {
        String[] fieldsInfo = new String[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            fieldsInfo[i] =
                    String.format(
                            "%s<%s>",
                            seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i));
        }
        return StringUtils.join(fieldsInfo, ", ");
    }

    private String fieldToString(SeaTunnelDataType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                List<String> arrayData = new ArrayList<>();
                for (int i = 0; i < Array.getLength(value); i++) {
                    arrayData.add(String.valueOf(Array.get(value, i)));
                }
                return arrayData.toString();
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                List<String> rowData = new ArrayList<>();
                SeaTunnelRowType rowType = (SeaTunnelRowType) type;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    rowData.add(
                            fieldToString(
                                    rowType.getFieldTypes()[i],
                                    ((SeaTunnelRow) value).getField(i)));
                }
                return rowData.toString();
            case STRING:
                return "'" + String.valueOf(value) + "'";
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return String.valueOf(value);
            default:
                return String.valueOf(value);
        }
    }

    private void batchCommit() {
        try {
            if(this.connection==null){
                String userName = this.jdbcSinkConfig.getUser();
                String password = this.jdbcSinkConfig.getPassWord();
                String url = this.jdbcSinkConfig.getUrl();
                try {
                    this.connection = DriverManager.getConnection(url, userName, password);
                    this.dslContext = DSL.using(connection, SQLDialect.MYSQL);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            this.dslContext.execute(this.delSql);
            this.dslContext.execute(this.insertSql);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
        }

    }
}
