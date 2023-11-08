/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qh.sink;

import com.google.auto.service.AutoService;
import com.qh.config.JdbcSinkConfig;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.qh.config.JdbcOptions.*;

@AutoService(Factory.class)
public class MySinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "XjJdbc";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, DB_TYPE)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        BATCH_SIZE,
                        BATCH_INTERVAL_MS,
                        IS_EXACTLY_ONCE,
                        GENERATE_SINK_SQL,
                        AUTO_COMMIT,
                        SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST,
                        FIELD_MAPPER,
                        PRE_CONFIG,
                        PRIMARY_KEYS)
                .conditional(
                        IS_EXACTLY_ONCE,
                        true,
                        XA_DATA_SOURCE_CLASS_NAME,
                        MAX_COMMIT_ATTEMPTS,
                        TRANSACTION_TIMEOUT_SEC)
                .conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
//                .conditional(GENERATE_SINK_SQL, true, DATABASE)
//                .conditional(GENERATE_SINK_SQL, false, QUERY)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        Optional<String> optionalTable = config.getOptional(TABLE);
        if (!optionalTable.isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(TABLE.key(), catalogTable.getTableId().getTableName());

            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            if (primaryKey != null && !CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
                map.put(PRIMARY_KEYS.key(), String.join(",", primaryKey.getColumnNames()));
            }
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        return () ->
                new MySink(
                        context.getCatalogTable().getTableSchema().toPhysicalRowDataType(),
                        options,
                        sinkConfig
                );
    }
}
