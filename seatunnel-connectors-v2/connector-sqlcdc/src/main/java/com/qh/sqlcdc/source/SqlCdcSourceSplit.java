package com.qh.sqlcdc.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Data;

@Data
public class SqlCdcSourceSplit implements SourceSplit {
    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }
}
