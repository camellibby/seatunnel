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

package com.qh.sqlcdc.dialect;


import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.converter.JdbcRowConverter;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */
public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();


    JdbcRowConverter getRowConverter();


    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

    default String quoteIdentifier(String identifier) {
        return identifier;
    }

    default String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    default String getColumnDistinctCount(String columnName, SqlCdcConfig config) {
        String template = "SELECT COUNT(DISTINCT <columnName>) sl " +
                "          FROM (<tableName>) a where <columnName> is not null";
        ST st = new ST(template);
        st.add("columnName", columnName);
        st.add("tableName", config.getQuery());
        return st.render();
    }

    default String getPartitionSql(String partitionColumn, String nativeSql) {
        return String.format(
                "SELECT * FROM (%s) tt where %s >= ? AND %s <= ?",
                nativeSql, partitionColumn, partitionColumn);
    }


    default String getHangValueSql(SqlCdcConfig sqlCdcConfig, long hang) {
        String template = "SELECT <columnName>" +
                "  FROM (SELECT <columnName>, ROW_NUMBER() OVER(ORDER BY <columnName> ASC) AS hang" +
                "          FROM (SELECT DISTINCT <columnName> FROM (<query>) A) A" +
                "         WHERE <columnName> IS NOT NULL) A" +
                " WHERE hang = <hang>";
        ST st = new ST(template);
        st.add("columnName", sqlCdcConfig.getPartitionColumn());
        st.add("query", sqlCdcConfig.getQuery());
        st.add("hang", hang);
        return st.render();
    }

    default String checkRealDelete(List<String> keys, String nativeSql) {
        String template = "select count(1) sl from (<query>) a where <keys:{key | <key> = ? }; separator=\" and \"> ";
        ST st = new ST(template);
        st.add("keys", keys);
        st.add("query", nativeSql);
        return st.render();
    }


}
