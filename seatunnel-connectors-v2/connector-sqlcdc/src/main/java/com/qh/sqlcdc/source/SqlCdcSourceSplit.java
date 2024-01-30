package com.qh.sqlcdc.source;

import lombok.Data;
import org.apache.seatunnel.api.source.SourceSplit;
@Data
public class SqlCdcSourceSplit implements SourceSplit {
    private final String splitId;
    @Override
    public String splitId() {
        return splitId;
    }
}
