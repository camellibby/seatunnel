package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
        return new ClickHouseTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    public String getUpdateStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] conditionFields,
            boolean isPrimaryKeyUpdated) {
        fieldNames =
                Arrays.stream(fieldNames)
                        .filter(
                                fieldName ->
                                        isPrimaryKeyUpdated
                                        || !Arrays.asList(conditionFields)
                                                .contains(fieldName))
                        .toArray(String[]::new);

        Set<String> set1 = new HashSet<>(Arrays.asList(fieldNames));
        Set<String> set2 = new HashSet<>(Arrays.asList(conditionFields));

        // 差集
        Set<String> difference = new HashSet<>(set1);
        difference.removeAll(set2);
        String[] newFieldNames = difference.toArray(new String[0]);


        String setClause =
                Arrays.stream(newFieldNames)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "UPDATE %s SET %s WHERE %s",
                tableIdentifier(database, tableName), setClause, conditionClause);
    }

}
