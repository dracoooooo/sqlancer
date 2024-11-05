package sqlancer.mariadb;

import sqlancer.common.query.SQLQueryAdapter;

import java.sql.*;
import java.util.*;

public class MySQLToMariaDBMigrator {

    public static void migrateSchema(Connection mysqlConn, Connection mariadbConn, String databaseName, MariaDBProvider.MariaDBGlobalState globalState)
            throws SQLException {
        // 1. get table structures from MySQL
        List<TableInfo> tables = getTableStructures(mysqlConn, databaseName);

        // 2. create tables in MariaDB
        createTablesInMariaDB(mariadbConn, tables,globalState);

    }

    private static class TableInfo {
        String name;
        String engine;
        List<ColumnInfo> columns;
        List<IndexInfo> indexes;
        List<ForeignKeyInfo> foreignKeys;
        String createTableSQL;
    }

    private static class ColumnInfo {
        String name;
        String type;
        boolean nullable;
        String defaultValue;
        boolean isPrimaryKey;
        String extra; // AUTO_INCREMENT等
    }

    private static class IndexInfo {
        String name;
        boolean isUnique;
        List<String> columns;
    }

    private static class ForeignKeyInfo {
        String name;
        String columnName;
        String referenceTable;
        String referenceColumn;
    }

    private static List<TableInfo> getTableStructures(Connection mysqlConn, String databaseName)
            throws SQLException {
        List<TableInfo> tables = new ArrayList<>();

        // get table names and engines
        String tableQuery = "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES " +
                "WHERE TABLE_SCHEMA = ?";

        try (PreparedStatement stmt = mysqlConn.prepareStatement(tableQuery)) {
            stmt.setString(1, databaseName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TableInfo table = new TableInfo();
                    table.name = rs.getString("TABLE_NAME");
                    table.engine = rs.getString("ENGINE");
                    table.columns = getColumns(mysqlConn, databaseName, table.name);
                    table.indexes = getIndexes(mysqlConn, databaseName, table.name);
                    table.foreignKeys = getForeignKeys(mysqlConn, databaseName, table.name);
                    table.createTableSQL = generateCreateTableSQL(mysqlConn, databaseName, table.name);
                    tables.add(table);
                }
            }
        }

        return tables;
    }

    private static List<ColumnInfo> getColumns(Connection conn, String dbName, String tableName)
            throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();

        String columnQuery = "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, " +
                "COLUMN_KEY, EXTRA FROM information_schema.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                "ORDER BY ORDINAL_POSITION";

        try (PreparedStatement stmt = conn.prepareStatement(columnQuery)) {
            stmt.setString(1, dbName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ColumnInfo col = new ColumnInfo();
                    col.name = rs.getString("COLUMN_NAME");
                    col.type = rs.getString("COLUMN_TYPE");
                    col.nullable = "YES".equals(rs.getString("IS_NULLABLE"));
                    col.defaultValue = rs.getString("COLUMN_DEFAULT");
                    col.isPrimaryKey = "PRI".equals(rs.getString("COLUMN_KEY"));
                    col.extra = rs.getString("EXTRA");
                    columns.add(col);
                }
            }
        }

        return columns;
    }

    private static List<IndexInfo> getIndexes(Connection conn, String dbName, String tableName)
            throws SQLException {
        List<IndexInfo> indexes = new ArrayList<>();
        Map<String, IndexInfo> indexMap = new HashMap<>();

        String indexQuery = "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE " +
                "FROM information_schema.STATISTICS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX";

        try (PreparedStatement stmt = conn.prepareStatement(indexQuery)) {
            stmt.setString(1, dbName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    IndexInfo index = indexMap.computeIfAbsent(indexName, k -> {
                        IndexInfo idx = new IndexInfo();
                        idx.name = k;
                        try {
                            idx.isUnique = rs.getInt("NON_UNIQUE") == 0;
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        idx.columns = new ArrayList<>();
                        return idx;
                    });
                    index.columns.add(rs.getString("COLUMN_NAME"));
                }
            }
        }

        indexes.addAll(indexMap.values());
        return indexes;
    }

    private static List<ForeignKeyInfo> getForeignKeys(Connection conn, String dbName, String tableName)
            throws SQLException {
        List<ForeignKeyInfo> foreignKeys = new ArrayList<>();

        String fkQuery = "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, " +
                "REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                "AND REFERENCED_TABLE_NAME IS NOT NULL";

        try (PreparedStatement stmt = conn.prepareStatement(fkQuery)) {
            stmt.setString(1, dbName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ForeignKeyInfo fk = new ForeignKeyInfo();
                    fk.name = rs.getString("CONSTRAINT_NAME");
                    fk.columnName = rs.getString("COLUMN_NAME");
                    fk.referenceTable = rs.getString("REFERENCED_TABLE_NAME");
                    fk.referenceColumn = rs.getString("REFERENCED_COLUMN_NAME");
                    foreignKeys.add(fk);
                }
            }
        }

        return foreignKeys;
    }

    private static String generateCreateTableSQL(Connection conn, String dbName, String tableName)
            throws SQLException {
        String sql = "";
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + dbName + "." + tableName)) {
                if (rs.next()) {
                    sql = rs.getString(2);
                }
            }
        }
        return convertMySQLToMariaDBSQL(sql);
    }

    private static String convertMySQLToMariaDBSQL(String mysqlSQL) {
        // todo
        String mariadbSQL = mysqlSQL
                .replaceAll("ENGINE=InnoDB", "ENGINE=Aria")
                .replaceAll("CHARSET=utf8mb4", "CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

        return mariadbSQL;
    }

    private static void createTablesInMariaDB(Connection mariadbConn, List<TableInfo> tables, MariaDBProvider.MariaDBGlobalState globalState)
            throws SQLException {
        // sort tables by dependency
        List<TableInfo> sortedTables = sortTablesByDependency(tables);

        for (TableInfo table : sortedTables) {

            try {
                String stmt = table.createTableSQL;
                globalState.executeStatement(new SQLQueryAdapter(stmt));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    private static List<TableInfo> sortTablesByDependency(List<TableInfo> tables) {
        // sort tables topologically
        Map<String, TableInfo> tableMap = new HashMap<>();
        Map<String, Set<String>> dependencies = new HashMap<>();

        for (TableInfo table : tables) {
            tableMap.put(table.name, table);
            dependencies.put(table.name, new HashSet<>());
        }

        for (TableInfo table : tables) {
            for (ForeignKeyInfo fk : table.foreignKeys) {
                dependencies.get(table.name).add(fk.referenceTable);
            }
        }

        List<TableInfo> sorted = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> visiting = new HashSet<>();

        for (TableInfo table : tables) {
            if (!visited.contains(table.name)) {
                topologicalSort(table.name, tableMap, dependencies, visited, visiting, sorted);
            }
        }

        return sorted;
    }

    private static void topologicalSort(String tableName,
                                        Map<String, TableInfo> tableMap,
                                        Map<String, Set<String>> dependencies,
                                        Set<String> visited,
                                        Set<String> visiting,
                                        List<TableInfo> sorted) {
        visiting.add(tableName);

        for (String dep : dependencies.get(tableName)) {
            if (visiting.contains(dep)) {
                throw new RuntimeException("Circular dependency detected");
            }
            if (!visited.contains(dep)) {
                topologicalSort(dep, tableMap, dependencies, visited, visiting, sorted);
            }
        }

        visiting.remove(tableName);
        visited.add(tableName);
        sorted.add(tableMap.get(tableName));
    }

}
