package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import javax.annotation.Nullable;
import java.util.List;

public class FlinkSQLEngine implements SQLEngine {
    private String inputTableName;
    @Nullable
    private String catalogTableName;
    private SeaTunnelRowType inputRowType;

    private String sql;

    @Override
    public void init(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            String sql) {
        this.inputTableName = inputTableName;
        this.catalogTableName = catalogTableName;
        this.inputRowType = inputRowType;
        this.sql = sql;
    }

    @Override
    public SeaTunnelRowType typeMapping(List<String> inputColumnsMapping) {
        return null;
    }

    @Override
    public SeaTunnelRow transformBySQL(SeaTunnelRow inputRow) {
        return inputRow;
    }
}
