package org.apache.seatunnel.connectors.seatunnel.hive.source;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.hive.config.MyHiveConfig;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.sql.*;
import java.util.ArrayList;

import static org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE;

@AutoService(SeaTunnelSource.class)
@Slf4j
public class MyHiveSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelRowType typeInfo;
    private JobContext jobContext;
    private MyHiveConfig hiveConfig;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.hiveConfig = new MyHiveConfig(pluginConfig);
        this.typeInfo = initTableField();
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
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new MyHiveReader(this.hiveConfig, readerContext, typeInfo);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }


    private SeaTunnelRowType initTableField() {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            String password = null;
            if (null != hiveConfig.getPassword() && !hiveConfig.getPassword().equalsIgnoreCase("")) {
                password = hiveConfig.getPassword();
            }
            DriverManager.setLoginTimeout(10);
            Connection conn = DriverManager.getConnection(hiveConfig.getJdbc_url(), hiveConfig.getUser(), password);

            String tableName = this.hiveConfig.getTableName();
            String sql = String.format("select * from %s qinhuanbieming where 1=2", tableName);
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                fieldNames.add(metaData.getColumnName(i).replace("qinhuanbieming.", ""));
                seaTunnelDataTypes.add(mapping(metaData, i));
            }
            ps.close();
            conn.close();
        } catch (Exception e) {
            log.warn("get row type info exception", e);
            throw new RuntimeException(e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (mysqlType) {
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "SMALLINT":
            case "INT":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "FLOAT":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "DEICIMAL":
                return new DecimalType(38, 18);
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return BasicType.STRING_TYPE;
            case "BINARY":
                return PrimitiveByteArrayType.INSTANCE;
            case "TIMESTAMP":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "DATE":
                return LocalTimeType.LOCAL_DATE_TYPE;

            // Doesn't support yet
            case "ARRAY":
                return STRING_ARRAY_TYPE;
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new RuntimeException(
                        String.format(
                                "Doesn't support MySQL type '%s' on column '%s'  yet.",
                                mysqlType, jdbcColumnName));
        }
    }
}
