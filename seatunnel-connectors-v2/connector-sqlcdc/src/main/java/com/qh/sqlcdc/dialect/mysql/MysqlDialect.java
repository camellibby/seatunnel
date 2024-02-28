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

package com.qh.sqlcdc.dialect.mysql;


import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.converter.JdbcRowConverter;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;
import org.stringtemplate.v4.ST;


public class MysqlDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new MysqlJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new MySqlTypeMapper();
    }


    public String getHangValueSql(SqlCdcConfig sqlCdcConfig, long hang) {
        String template = "SELECT <columnName>   " +
                "FROM  " +
                "  (  " +
                "  SELECT <columnName>  " +
                "    ,  " +
                "    @rownum := @rownum + 1 hang   " +
                "  FROM  " +
                "    ( SELECT DISTINCT <columnName> FROM ( <query> ) A ORDER BY <columnName> ASC ) a,  " +
                "    ( SELECT @rownum := 0 ) b   " +
                "  ) a   " +
                "WHERE  " +
                "  hang = <hang>";
        ST st = new ST(template);
        st.add("columnName", sqlCdcConfig.getPartitionColumn());
        st.add("query", sqlCdcConfig.getQuery());
        st.add("hang", hang);
        return st.render();
    }


}
