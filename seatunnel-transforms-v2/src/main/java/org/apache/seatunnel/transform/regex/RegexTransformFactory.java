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

package org.apache.seatunnel.transform.regex;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;

@AutoService(Factory.class)
public class RegexTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "Regex";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        RegexTransformConfig.KEY_REGEX,
                        RegexTransformConfig.KEY_OUTPUT_FIELDS)
                .build();
    }

    @Override
    public TableTransform createTransform(@NonNull TableTransformFactoryContext context) {
        RegexTransformConfig regexTransformConfig = RegexTransformConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        return () -> new RegexTransform(regexTransformConfig, catalogTable);
    }
}