package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLTable;

public class MySQLTableReference implements MySQLFromItem {
    public String getName() {
        return table.getName();
    }

    private final MySQLTable table;

    public MySQLTableReference(MySQLTable table) {
        this.table = table;
    }

    public MySQLTable getTable() {
        return table;
    }

}
