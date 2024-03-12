package com.qh.myconnect.dialect.sqlserver;

import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.converter.JdbcRowConverter;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import com.qh.myconnect.dialect.mysql.MysqlJdbcRowConverter;
import com.qh.myconnect.dialect.pgsql.PostgresTypeMapper;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class SqlServerDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "SqlServer";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SqlServerJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SqlserverTypeMapper();
    }


    public String copyTableOnlyColumn(String sourceTable, String targetTable, JdbcSinkConfig jdbcSinkConfig) {
        return format("select  %s into %s from %s where 1=2 ",
                StringUtils.join(jdbcSinkConfig.getPrimaryKeys(), ','),
                targetTable,
                sourceTable
        );
    }

    @Override
    public Optional<String> getUpsertStatement(String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {

        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "[SOURCE]." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "MERGE INTO %s.%s AS [TARGET]"
                                + " USING (%s) AS [SOURCE]"
                                + " ON (%s)"
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s);",
                        database,
                        tableName,
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }
}
