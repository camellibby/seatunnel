package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.List;

public class FlinkSQLEngine implements SQLEngine {
    private List<String> inputTableName;
    private SeaTunnelRowType inputRowType;
    private String sql;

    @Override
    public void init(List<String> inputTableName, SeaTunnelRowType inputRowType, String sql) {
        this.inputTableName = inputTableName;
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
