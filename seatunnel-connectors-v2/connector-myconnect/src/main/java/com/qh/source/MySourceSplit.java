package com.qh.source;

import com.qh.config.MySourceOptions;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.api.source.SourceSplit;

@Data
@AllArgsConstructor
public class MySourceSplit implements SourceSplit {
    private int splitId;
    MySourceOptions.DbConfig dbConfig;

    @Override
    public String splitId() {
        return String.valueOf(splitId);
    }
}
