package com.qh.myconnect.source;

import com.qh.myconnect.config.MySourceOptions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;


public class MySourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "XjSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MySourceOptions.DB_CONFIGS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MySource.class;
    }
}
