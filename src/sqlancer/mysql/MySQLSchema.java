package sqlancer.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;
import sqlancer.mysql.ast.MySQLConstant;

public class MySQLSchema extends AbstractSchema<MySQLGlobalState, MySQLTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;
    private List<MySQLEdge> edges;

    public enum MySQLDataType {
        BOOLEAN, INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static MySQLDataType getRandom(MySQLGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(MySQLDataType.INT, MySQLDataType.VARCHAR);
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case BOOLEAN:
                case INT:
                case DOUBLE:
                case FLOAT:
                case DECIMAL:
                    return true;
                case VARCHAR:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

    }

    public static class MySQLColumn extends AbstractTableColumn<MySQLTable, MySQLDataType> {

        private final boolean isPrimaryKey;
        private boolean isForeignKey;
        private MySQLColumn refColumn;
        private final int precision;
        private final String exactType; // the exact type information

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MySQLColumn(String name, MySQLDataType columnType, String exactType, boolean isPrimaryKey, int precision) {
            super(name, null, columnType);
            this.exactType = exactType;
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public void setForeignKey(boolean isForeignKey, MySQLColumn refColumn) {
            this.isForeignKey = isForeignKey;
            this.refColumn = refColumn;
        }

        public boolean isForeignKey() {
            return isForeignKey;
        }

        public MySQLColumn getRefColumn() {
            return refColumn;
        }

        public String getExactType() {
            return exactType;
        }

        public boolean canBeUsedAsFK() {
            return getType() != MySQLDataType.VARCHAR || !exactType.toLowerCase().contains("text");
        }
    }

    public static class MySQLTables extends AbstractTables<MySQLTable, MySQLColumn> {

        public MySQLTables(List<MySQLTable> tables) {
            super(tables);
        }

        public MySQLRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                            c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<MySQLColumn, MySQLConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    MySQLColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    MySQLConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = MySQLConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = MySQLConstant.createIntConstant((long) value);
                                break;
                            case VARCHAR:
                                value = randomRowValues.getString(columnIndex);
                                constant = MySQLConstant.createStringConstant((String) value);
                                break;
                            default:
                                throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new MySQLRowValue(this, values);
            }

        }

    }

    private static MySQLDataType getColumnType(String typeString) {
        switch (typeString) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
            case "bigint":
                return MySQLDataType.INT;
            case "varchar":
            case "tinytext":
            case "mediumtext":
            case "text":
            case "longtext":
                return MySQLDataType.VARCHAR;
            case "double":
                return MySQLDataType.DOUBLE;
            case "float":
                return MySQLDataType.FLOAT;
            case "decimal":
                return MySQLDataType.DECIMAL;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class MySQLRowValue extends AbstractRowValue<MySQLTables, MySQLColumn, MySQLConstant> {

        MySQLRowValue(MySQLTables tables, Map<MySQLColumn, MySQLConstant> values) {
            super(tables, values);
        }

    }

    public static class MySQLTable extends AbstractRelationalTable<MySQLColumn, MySQLIndex, MySQLGlobalState> {

        public enum MySQLEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private String s;

            MySQLEngine(String s) {
                this.s = s;
            }

            public static MySQLEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final MySQLEngine engine;

        public MySQLTable(String tableName, List<MySQLColumn> columns, List<MySQLIndex> indexes, MySQLEngine engine) {
            super(tableName, columns, indexes, false /* TODO: support views */);
            this.engine = engine;
        }

        public MySQLEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

        public MySQLColumn getPrimaryKey() {
            if (hasPrimaryKey()) {
                return getColumns().stream().filter(c -> c.isPrimaryKey()).findAny().get();
            } else {
                return null;
            }
        }

    }

    public static final class MySQLIndex extends TableIndex {

        private MySQLIndex(String indexName) {
            super(indexName);
        }

        public static MySQLIndex create(String indexName) {
            return new MySQLIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static class MySQLEdge {
        private static int COUNT = 0;
        private final String name; // Just a name, used in translator
        private final MySQLTable sourceTable;
        private final MySQLColumn sourceColumn;
        private final MySQLTable targetTable;
        private final MySQLColumn targetColumn;

        public MySQLEdge(MySQLTable sourceTable, MySQLColumn sourceColumn,
                         MySQLTable targetTable, MySQLColumn targetColumn) {
            this.name = "Relation" + COUNT++;
            this.sourceTable = sourceTable;
            this.sourceColumn = sourceColumn;
            this.targetTable = targetTable;
            this.targetColumn = targetColumn;
        }

        public MySQLTable getSourceTable() {
            return sourceTable;
        }

        public MySQLColumn getSourceColumn() {
            return sourceColumn;
        }

        public MySQLTable getTargetTable() {
            return targetTable;
        }

        public MySQLColumn getTargetColumn() {
            return targetColumn;
        }
    }

    public static MySQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.mysql.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MySQLTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            MySQLEngine engine = MySQLEngine.get(tableEngineStr);
                            List<MySQLColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<MySQLIndex> indexes = getIndexes(con, tableName, databaseName);
                            MySQLTable t = new MySQLTable(tableName, databaseColumns, indexes, engine);
                            for (MySQLColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                List<MySQLEdge> edges = fromConnectionGetEdges(con, databaseName, databaseTables);
                return new MySQLSchema(databaseTables, edges);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }

        throw new AssertionError(ex);
    }

    private static List<MySQLEdge> fromConnectionGetEdges(SQLConnection con, String databaseName,
                                                          List<MySQLTable> tables) throws SQLException {
        List<MySQLEdge> edges = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            String query = "select TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
                    "from information_schema.KEY_COLUMN_USAGE " +
                    "where REFERENCED_TABLE_SCHEMA = '" + databaseName + "' " +
                    "and REFERENCED_TABLE_NAME is not null";

            try (ResultSet rs = s.executeQuery(query)) {
                while (rs.next()) {
                    String sourceTableName = rs.getString("TABLE_NAME");
                    String sourceColumnName = rs.getString("COLUMN_NAME");
                    String targetTableName = rs.getString("REFERENCED_TABLE_NAME");
                    String targetColumnName = rs.getString("REFERENCED_COLUMN_NAME");

                    MySQLTable sourceTable = findTable(tables, sourceTableName);
                    MySQLTable targetTable = findTable(tables, targetTableName);

                    if (sourceTable != null && targetTable != null) {
                        MySQLColumn sourceColumn = findColumn(sourceTable, sourceColumnName);
                        MySQLColumn targetColumn = findColumn(targetTable, targetColumnName);

                        if (sourceColumn != null && targetColumn != null) {
                            sourceColumn.setForeignKey(true, targetColumn);
                            edges.add(new MySQLEdge(sourceTable, sourceColumn, targetTable, targetColumn));
                        }
                    }
                }
            }
        }
        return edges;
    }

    private static MySQLTable findTable(List<MySQLTable> tables, String tableName) {
        return tables.stream()
                .filter(t -> t.getName().equals(tableName))
                .findFirst()
                .orElse(null);
    }

    private static MySQLColumn findColumn(MySQLTable table, String columnName) {
        return table.getColumns().stream()
                .filter(c -> c.getName().equals(columnName))
                .findFirst()
                .orElse(null);
    }

    private static List<MySQLIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(MySQLIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MySQLColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    String exactColumnType = rs.getString("COLUMN_TYPE"); // Get exact type info
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    MySQLDataType columnType = getColumnType(dataType);
                    // special handling for tinyint(1)
                    if (exactColumnType.equals("tinyint(1)")) {
                        columnType = MySQLDataType.BOOLEAN;
                    }
                    MySQLColumn c = new MySQLColumn(columnName, columnType, exactColumnType, isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public MySQLSchema(List<MySQLTable> databaseTables) {
        super(databaseTables);
    }

    public MySQLSchema(List<MySQLTable> databaseTables, List<MySQLEdge> edges) {
        super(databaseTables);
        this.edges = edges;
    }

    public List<MySQLEdge> getEdges() {
        return edges;
    }

    public MySQLEdge getRandomEdge() {
        if (edges.isEmpty()) {
            return null;
        }
        return Randomly.fromList(edges);
    }

    public List<MySQLEdge> getRandomConnectedEdges() {
        List<MySQLEdge> connectedEdges = new ArrayList<>();
        List<MySQLTable> nodeTables = new ArrayList<>();

        if(edges.isEmpty()) {
            return connectedEdges;
        }
        MySQLEdge firstEdge = getRandomEdge();
        List<MySQLEdge> availableEdges = new ArrayList<>(edges);
        availableEdges.remove(firstEdge);
        connectedEdges.add(firstEdge);

        nodeTables.add(firstEdge.getSourceTable());
        nodeTables.add(firstEdge.getTargetTable());

        while (!availableEdges.isEmpty()) {
            MySQLEdge edge = Randomly.fromList(availableEdges);
            if ((nodeTables.contains(edge.getSourceTable()) && !nodeTables.contains(edge.getTargetTable()))
                    || (nodeTables.contains(edge.getTargetTable()) && !nodeTables.contains(edge.getSourceTable()))) {
                connectedEdges.add(edge);
                availableEdges.remove(edge);
                if (!nodeTables.contains(edge.getSourceTable())) {
                    nodeTables.add(edge.getSourceTable());
                } else {
                    nodeTables.add(edge.getTargetTable());
                }
                if (Randomly.getBoolean()) {
                    break;
                }
            } else {
                if(Randomly.getBooleanWithRatherLowProbability()){
                    break;
                }
            }

        }
        return connectedEdges;
    }

    public MySQLTables getRandomTableNonEmptyTables() {
        return new MySQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public MySQLTable getTableByName(String tableName) {
        return getDatabaseTables().stream().filter(t -> t.getName().contentEquals(tableName)).findAny().get();
    }

    /**
     * determine if a table can be referenced
     *
     * @param table table to be checked
     * @return true if the table can be referenced, otherwise false
     */
    public boolean canBeReferencedTable(MySQLTable table) {
        // check for primary key
        boolean hasPrimaryKey = table.getColumns().stream().anyMatch(MySQLColumn::isPrimaryKey);

        // check for referencable column
        boolean hasReferencableColumn = table.getColumns().stream().anyMatch(this::canBeReferencedColumn);

        return hasPrimaryKey && hasReferencableColumn;
    }

    /**
     * determine if a column can be referenced
     *
     * @param column column to be checked
     * @return true if the column can be referenced, otherwise false
     */
    public boolean canBeReferencedColumn(MySQLColumn column) {
        // check for primary key
        if (!column.isPrimaryKey()) {
            return false;
        }

        // check for column type
        if (column.getType() == MySQLDataType.VARCHAR) {
            // exclude TEXT type
            if (column.getExactType().toLowerCase().contains("text")) {
                return false;
            }
        }

        // exclude BLOB type
        if (column.getExactType().toLowerCase().contains("blob")) {
            return false;
        }

        return true;
    }


}
