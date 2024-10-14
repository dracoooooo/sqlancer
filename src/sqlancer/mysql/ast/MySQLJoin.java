package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLTable;

public class MySQLJoin implements MySQLExpression {

    public enum JoinType {
        NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS, FULL;
    }

    private final MySQLTable table1;
    private final MySQLTable table2;
    private MySQLExpression onClause;
    private JoinType type;
    private boolean isFirstJoin;

    public MySQLJoin(MySQLTable table1, MySQLTable table2, MySQLExpression onClause, JoinType type, boolean isFirstJoin) {
        this.table1 = table1;
        this.table2 = table2;
        this.onClause = onClause;
        this.type = type;
        this.isFirstJoin = isFirstJoin;
    }

    public MySQLTable getLeftTable() {
        return table1;
    }

    public MySQLTable getRightTable() {
        return table2;
    }

    public MySQLExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    public void setOnClause(MySQLExpression onClause) {
        this.onClause = onClause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public boolean isFirstJoin() {
        return isFirstJoin;
    }
}
