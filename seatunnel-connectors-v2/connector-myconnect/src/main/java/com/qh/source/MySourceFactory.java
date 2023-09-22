package com.qh.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import static com.qh.config.MySourceOptions.DB_CONFIGS;


public class MySourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "XjSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(DB_CONFIGS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MySource.class;
    }
}
