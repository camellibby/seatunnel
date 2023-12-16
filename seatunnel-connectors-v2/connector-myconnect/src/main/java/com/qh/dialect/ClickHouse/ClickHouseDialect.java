package com.qh.dialect.ClickHouse;

import com.qh.config.JdbcSinkConfig;
import com.qh.converter.ColumnMapper;
import com.qh.converter.JdbcRowConverter;
import com.qh.dialect.JdbcDialect;
import com.qh.dialect.JdbcDialectTypeMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.stringtemplate.v4.ST;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
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
    public Optional<String> getUpsertStatement(String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }


    public String getSinkQueryZipper(String tableName, List<ColumnMapper> columnMappers, int rowSize) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString = " select " +
                " <columns:{sub |   <if(sub.uc)> <sub.sinkColumnName> <else> argMax( <sub.sinkColumnName>, operateTime) as  <sub.sinkColumnName>  <endif>   }; separator=\", \"> " +
                "  from <table> " +
                " where operateFlag in ('I', 'U') " +
                " and <filter> " +
                " group by <ucs:{uc | <uc.sinkColumnName>   }; separator=\", \"> " +
                " order by <ucs:{uc | <uc.sinkColumnName>   }; separator=\" ,\"> ";

        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", tableName);
        sqlQueryTemplate.add("columns", columnMappers);
        sqlQueryTemplate.add("ucs", ucColumns);
        if (rowSize == 0) {
            sqlQueryTemplate.add("filter", "1=2");
        } else {
            String where = "(";
            List<String> collect = ucColumns.stream().map(x -> x.getSinkColumnName()).collect(Collectors.toList());
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

    public String copyTableOnlyColumn(String sourceTable, String targetTable, List<String> columns) {
        return format("create  table %s ENGINE = MergeTree order by (%s) as  select  %s from %s where 1=2 ",
                targetTable,
                StringUtils.join(columns, ','),
                StringUtils.join(columns, ','),
                sourceTable
        );
    }

    public void updateData(Connection connection,
                           String table,
                           List<ColumnMapper> columnMappers,
                           List<ColumnMapper> listUc,
                           HashMap<List<String>, SeaTunnelRow> rows,
                           Map<String, String> metaDataHash
    ) throws SQLException {
        String templateInsert = "update <table> set " +
                "<columns:{sub | <sub.sinkColumnName> = ? }; separator=\", \"> " +
                " where  <pks:{pk | <pk.sinkColumnName> = ? }; separator=\" and \"> ";
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

    public int deleteData(Connection connection, String table, String ucTable, List<ColumnMapper> ucColumns) {

        String querySql = "select count(1) sl from  <table>   WHERE (<pks:{pk | <pk.sinkColumnName>}; separator=\", \">) NOT IN   (SELECT  <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM <ucTable>  ) ";
        ST st = new ST(querySql);
        st.add("table", table);
        st.add("ucTable", ucTable);
        st.add("pks", ucColumns);
        PreparedStatement query = null;
        int del = 0;

        String delSql = "ALTER  TABLE <table> DELETE  WHERE (<pks:{pk | <pk.sinkColumnName>}; separator=\", \">) NOT IN   (SELECT  <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM <ucTable>  ) ";
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

    public int deleteDataZipper(Connection connection, JdbcSinkConfig jdbcSinkConfig, List<ColumnMapper> columnMappers, LocalDateTime startTime) {
        List<ColumnMapper> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        int insert = 0;
        String insertSql1 = "select  count(1) sl  " +
                "    from (select <columns:{sub |   <if(sub.uc)> <sub.sinkColumnName> <else> argMax( <sub.sinkColumnName>, operateTime) as  <sub.sinkColumnName>  <endif>   }; separator=\", \">  " +
                "            from <table>" +
                "           where operateFlag in ('I', 'U')" +
                "             AND (<pks:{pk | <pk.sinkColumnName>}; separator=\", \">) NOT IN (SELECT <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM <ucTable> ut)" +
                "           group by <pks:{pk | <pk.sinkColumnName>}; separator=\", \">" +
                "           order by <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> ) a";
        ST template1 = new ST(insertSql1);
        template1.add("table", jdbcSinkConfig.getTable());
        template1.add("columns", columnMappers);
        template1.add("pks", ucColumns);
        template1.add("ucTable", "UC_" + jdbcSinkConfig.getTable());
        template1.add("operateTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
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
        String insertSql = "insert into <table>" +
                "  (<columns:{sub | <sub.sinkColumnName>  }; separator=\", \">, operateFlag, operateTime)" +
                "  select <columns:{sub | <sub.sinkColumnName> }; separator=\", \">, 'D' operateFlag, '<operateTime>' operateTime" +
                "    from (select <columns:{sub |   <if(sub.uc)> <sub.sinkColumnName> <else> argMax( <sub.sinkColumnName>, operateTime) as  <sub.sinkColumnName>  <endif>   }; separator=\", \">  " +
                "            from <table>" +
                "           where operateFlag in ('I', 'U')" +
                "             AND (<pks:{pk | <pk.sinkColumnName>}; separator=\", \">) NOT IN (SELECT <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM <ucTable> ut)" +
                "           group by <pks:{pk | <pk.sinkColumnName>}; separator=\", \">" +
                "           order by <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> ) a";
        ST template = new ST(insertSql);
        template.add("table", jdbcSinkConfig.getTable());
        template.add("columns", columnMappers);
        template.add("pks", ucColumns);
        template.add("ucTable", "UC_" + jdbcSinkConfig.getTable());
        template.add("operateTime", startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
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
}
