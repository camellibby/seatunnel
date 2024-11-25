package com.qh.myconnect.dialect.ClickHouse;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.StringUtils;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.StringResourceLoader;
import org.apache.velocity.runtime.resource.util.StringResourceRepository;
import org.stringtemplate.v4.ST;

import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.converter.JdbcRowConverter;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class ClickHouseDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new ClickHouseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new ClickHouseMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    public String getSinkQueryZipper(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString =
                " select "
                + " <columns:{sub |   <if(sub.uc)> `<sub.sinkColumnName>` <else> argMax( `<sub.sinkColumnName>`, OPERATETIME) as  `<sub.sinkColumnName>`  <endif>   }; separator=\", \"> "
                + "  from `<table>` "
                + " where OPERATEFLAG in ('I', 'U') "
                + " and <filter> "
                + " group by <ucs:{uc | `<uc.sinkColumnName>`   }; separator=\", \"> "
                + " order by <ucs:{uc | `<uc.sinkColumnName>`   }; separator=\" ,\"> ";

        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        sqlQueryTemplate.add("ucs", ucColumns);
        if (rowSize == 0) {
            sqlQueryTemplate.add("filter", "1=2");
        }
        else {
            String where = "(";
            List<String> collect =
                    ucColumns.stream()
                            .map(x -> String.format("`%s`", x.getSinkColumnName()))
                            .collect(Collectors.toList());
            String join = StringUtils.join(collect, ',');
            where += join + ") in ( %s ) ";
            List<String> tmp1 = new ArrayList<>();
            for (int i1 = 0; i1 < ucColumns.size(); i1++) {
                tmp1.add("?");
            }
            String tmp2 = String.format("(%s)", StringUtils.join(tmp1, ","));

            List<String> tmp3 = new ArrayList<>();
            for (int i = 0; i < rowSize; i++) {
                tmp3.add(tmp2);
            }
            where = String.format(where, StringUtils.join(tmp3, ","));
            sqlQueryTemplate.add("filter", where);
        }
        String sqlQuery = sqlQueryTemplate.render();
        return sqlQuery;
    }

    public String dropTable(JdbcSinkConfig jdbcSinkConfig, String tableName) {
        return String.format("drop table  `%s`", tableName);
    }

    public String copyTableOnlyColumn(
            String sourceTable, String targetTable, JdbcSinkConfig jdbcSinkConfig) {
        List<String> collect =
                jdbcSinkConfig.getPrimaryKeys().stream()
                        .map(x -> "`" + x + "`")
                        .collect(Collectors.toList());
        return format(
                "create  table `%s` ENGINE = MergeTree ORDER BY tuple() as select  %s from `%s` where 1=2 ",
                targetTable, StringUtils.join(collect, ','), sourceTable);
    }

    public void updateData(
            Connection connection,
            String table,
            List<ColumnMapper> columnMappers,
            List<ColumnMapper> listUc,
            HashMap<List<String>, SeaTunnelRow> rows,
            Map<String, String> metaDataHash)
            throws SQLException {
        String templateInsert =
                "update <table> set "
                + "<columns:{sub | <sub.sinkColumnName> = ? }; separator=\", \"> "
                + " where  <pks:{pk | <pk.sinkColumnName> = ? }; separator=\" and \"> ";
        List<ColumnMapper> newColumnMappers = new ArrayList<>();
        for (ColumnMapper columnMapper : columnMappers) {
            newColumnMappers.add(columnMapper);
        }
        newColumnMappers.removeAll(listUc);
        ST template = new ST(templateInsert);
        template.add("table", table);
        template.add("columns", newColumnMappers);
        template.add("pks", listUc);
        String updateSql = template.render();
        PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
        for (SeaTunnelRow row : rows.values()) {
            for (int i = 0; i < newColumnMappers.size(); i++) {
                String column = newColumnMappers.get(i).getSinkColumnName();
                String dbType = metaDataHash.get(column);
                this.setPreparedStatementValueByDbType(
                        i + 1,
                        preparedStatement,
                        dbType,
                        (String) row.getField(newColumnMappers.get(i).getSinkRowPosition()));
            }
            for (int i = 0; i < listUc.size(); i++) {
                String column = listUc.get(i).getSinkColumnName();
                String dbType = metaDataHash.get(column);
                this.setPreparedStatementValueByDbType(
                        i + 1 + newColumnMappers.size(),
                        preparedStatement,
                        dbType,
                        (String) row.getField(listUc.get(i).getSinkRowPosition()));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    public int deleteData(
            Connection connection, String table, String ucTable, List<ColumnMapper> ucColumns) {

        String querySql =
                "select count(1) sl from  `<table>`   WHERE (<pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">) NOT IN   (SELECT  <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> FROM `<ucTable>`  ) ";
        ST st = new ST(querySql);
        st.add("table", table);
        st.add("ucTable", ucTable);
        st.add("pks", ucColumns);
        PreparedStatement query = null;
        int del = 0;

        String delSql =
                "ALTER  TABLE `<table>` DELETE  WHERE (<pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">) NOT IN   (SELECT  <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> FROM `<ucTable>`  ) ";
        ST template = new ST(delSql);
        template.add("table", table);
        template.add("ucTable", ucTable);
        template.add("pks", ucColumns);
        PreparedStatement preparedStatement = null;
        try {
            query = connection.prepareStatement(st.render());
            ResultSet resultSet = query.executeQuery();
            if (resultSet.next()) {
                del = resultSet.getInt("sl");
            }
            query.close();

            preparedStatement = connection.prepareStatement(template.render());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return del;
    }

    public int deleteDataOnCluster(
            Connection connection,
            String table,
            String ucTable,
            List<ColumnMapper> ucColumns,
            String clusterName) {

        String querySql =
                "select count(1) sl from  `<table>`   WHERE (<pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">) NOT IN   (SELECT  <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> FROM `<ucTable>`  ) ";
        ST st = new ST(querySql);
        st.add("table", table);
        st.add("ucTable", ucTable);
        st.add("pks", ucColumns);
        PreparedStatement query = null;
        int del = 0;

        String delSql =
                "ALTER  TABLE `<table>` on CLUSTER  <clusterName> DELETE   WHERE (<pks:{pk | `<pk.sinkColumnName>`}; separator=\", "
                + "\">) NOT IN   (SELECT  <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> FROM "
                + "`<ucTable>`  ) SETTINGS allow_nondeterministic_mutations = 1 ";
        ST template = new ST(delSql);
        template.add("table", table);
        template.add("ucTable", ucTable);
        template.add("pks", ucColumns);
        template.add("clusterName", clusterName);
        PreparedStatement preparedStatement = null;
        try {
            query = connection.prepareStatement(st.render());
            ResultSet resultSet = query.executeQuery();
            if (resultSet.next()) {
                del = resultSet.getInt("sl");
            }
            query.close();

            preparedStatement = connection.prepareStatement(template.render());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return del;
    }

    public int deleteDataZipper(
            Connection connection,
            JdbcSinkConfig jdbcSinkConfig,
            List<ColumnMapper> columnMappers,
            LocalDateTime startTime) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        int insert = 0;
        String insertSql1 =
                "select  count(1) sl  "
                + "    from (select <columns:{sub |   <if(sub.uc)> `<sub.sinkColumnName>` <else> argMax( `<sub.sinkColumnName>`, OPERATETIME) as  `<sub.sinkColumnName>`  <endif>   }; separator=\", \">  "
                + "            from `<table>`"
                + "           where OPERATEFLAG in ('I', 'U')"
                + "             AND (<pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">) NOT IN (SELECT <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> FROM `<ucTable>` ut)"
                + "           group by <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">"
                + "           order by <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> ) a";
        ST template1 = new ST(insertSql1);
        template1.add("table", jdbcSinkConfig.getTable());
        template1.add("columns", columnMappers);
        template1.add("pks", ucColumns);
        template1.add("ucTable", "XJ$_" + jdbcSinkConfig.getTable());
        template1.add(
                "OPERATETIME",
                startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String render1 = template1.render();
        PreparedStatement preparedStatement1 = null;
        try {
            preparedStatement1 = connection.prepareStatement(render1);
            ResultSet resultSet = preparedStatement1.executeQuery();
            if (resultSet.next()) {
                insert = resultSet.getInt("sl");
            }
            preparedStatement1.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String insertSql =
                "insert into `<table>`"
                + "  (<columns:{sub | `<sub.sinkColumnName>`  }; separator=\", \">, OPERATEFLAG, OPERATETIME)"
                + "  select <columns:{sub | `<sub.sinkColumnName>` }; separator=\", \">, 'D' OPERATEFLAG, '<OPERATETIME>' OPERATETIME"
                + "    from (select <columns:{sub |   <if(sub.uc)> `<sub.sinkColumnName>` <else> argMax( `<sub.sinkColumnName>`, OPERATETIME) as  `<sub.sinkColumnName>`  <endif>   }; separator=\", \">  "
                + "            from `<table>`"
                + "           where OPERATEFLAG in ('I', 'U')"
                + "             AND (<pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">) NOT IN (SELECT <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> FROM `<ucTable>` ut)"
                + "           group by <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \">"
                + "           order by <pks:{pk | `<pk.sinkColumnName>`}; separator=\", \"> ) a";
        ST template = new ST(insertSql);
        template.add("table", jdbcSinkConfig.getTable());
        template.add("columns", columnMappers);
        template.add("pks", ucColumns);
        template.add("ucTable", "XJ$_" + jdbcSinkConfig.getTable());
        template.add(
                "OPERATETIME",
                startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String render = template.render();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(render);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return insert;
    }

    public Long getTableCount(Connection connection, String table) {
        long count = 0L;
        try {
            PreparedStatement preparedStatement =
                    connection.prepareStatement(format("select  count(1) sl  from `%s`", table));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("sl");
                preparedStatement.close();
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getSinkQueryUpdate(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        /*
        String sqlQueryString =
                " select <columns:{sub | `<sub.sinkColumnName>`}; separator=\", \"> "
                + "  from `<table>` a "
                + " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        */
        String sqlQueryString =
                "#set($separator = '') "
                + " select "
                + "#foreach( $item in $columns )"
                + " $separator  `$item` "
                + "#set($separator = ',') "
                + "#end"
                + "  from `${table}` a "
                + " where  ";
        VelocityEngine ve = new VelocityEngine();
        ve.init();
        Velocity.init();
        VelocityContext context = new VelocityContext();
        context.put("table", jdbcSinkConfig.getTable());
        context.put("columns", columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
        StringWriter writer = new StringWriter();
        Velocity.evaluate(context, writer, "mystring2", sqlQueryString);
        String sqlQuery = writer.toString();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            /*
            String tmpWhere = "( <ucs:{uc | `<uc.sinkColumnName>` = ?  }; separator=\" and \">  )";
            ST tmpst = new ST(tmpWhere);
            tmpst.add("ucs", ucColumns);
            String render = tmpst.render();
            */
            String tmpWhere = "#set($separator2 = '') "
                              + "("
                              + "#foreach( $item in $pks )"
                              + " $separator2  `$item` = ?"
                              + "#set($separator2 = ' and  ') "
                              + "#end"
                              + ")";
            VelocityEngine ve2 = new VelocityEngine();
            ve2.init();
            Velocity.init();
            VelocityContext context2 = new VelocityContext();
            context2.put("pks", ucColumns.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList()));
            StringWriter writer2 = new StringWriter();
            Velocity.evaluate(context2, writer2, "mystring2", tmpWhere);
            where.add(writer2.toString());
        }
        String wheres = StringUtils.join(where, "  or ");
        if (rowSize == 0) {
            sqlQuery = sqlQuery + " 1=2";
        }
        else {
            sqlQuery = sqlQuery + wheres;
        }
        return sqlQuery;
    }
    @Override
    public String getUpdateStatement() {
        return "#set($separator = '') "
               + "#set($separator2 = '') "
               + "update `${table}` set "
               + "#foreach( $item in $columns )"
               + " $separator  `$item` = ?"
               + "#set($separator = ', ') "
               + "#end"
               + " where "
               + "#foreach( $item in $pks )"
               + " $separator2  `$item` = ?"
               + "#set($separator2 = ' and  ') "
               + "#end";
    }
    public String truncateTable(JdbcSinkConfig jdbcSinkConfig) {
        return String.format("truncate  table `%s`", jdbcSinkConfig.getTable());
    }
    public String insertTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        String sql =
                "insert into "
                + "`"
                + jdbcSinkConfig.getTable()
                + "`"
                + String.format(
                        "(%s)",
                        StringUtils.join(
                                columns.stream()
                                        .map(x -> String.format("`%s`", x))
                                        .collect(Collectors.toList()),
                                ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    public String insertTableOnlyColumn(JdbcSinkConfig jdbcSinkConfig, List<String> columns) {
        String sql =
                "insert into "
                + "`"
                + jdbcSinkConfig.getTable()
                + "`"
                + String.format(
                        "(%s)",
                        StringUtils.join(
                                columns.stream()
                                        .map(x -> String.format("`%s`", x))
                                        .collect(Collectors.toList()),
                                ","));
        return sql;
    }

    public ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSinkConfig jdbcSourceConfig)
            throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach(
                (k, v) -> {
                    columns.add(v);
                });
        String sql =
                String.format(
                        "select  %s from `%s` where 1=2 ",
                        StringUtils.join(
                                columns.stream()
                                        .map(x -> "`" + x + "`")
                                        .collect(Collectors.toList()),
                                ","),
                        table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }
}
