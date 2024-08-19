package sqlancer.mysql.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLTypedExpressionGenerator;

public class MySQLJoin implements MySQLExpression {

    public enum JoinType {
        NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS;
    }

    private final MySQLFromItem joinItem;//被连接的item,包含表或者子查询的结果
    private MySQLExpression onClause;//连接条件
    private JoinType type;//连接类型

    public MySQLJoin(MySQLJoin other) {
        this.joinItem = other.joinItem;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public MySQLJoin(MySQLFromItem joinItem, MySQLExpression onClause, JoinType type) {
        this.joinItem = joinItem;
        this.onClause = onClause;
        this.type = type;

    }

    public MySQLFromItem getJoinItem() {
        return joinItem;
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

    public static List<MySQLJoin> getRandomJoinClauses(List<MySQLTable> tables, MySQLGlobalState globalState) {
        List<MySQLJoin> joinStatements = new ArrayList<>();
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values()));
        List<MySQLColumn> columns = new ArrayList<>();
        if (tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }//多个连接子句与自然连接不兼容，因为它需要唯一的列名，而其他连接将生成重复的列名
            for (int i = 0; i < nrJoinClauses; i++) {
                MySQLFromItem joinItem = getRandomFromItem(tables, globalState);
                if(joinItem instanceof MySQLTableReference) {
                    MySQLTableReference tableReference = (MySQLTableReference) joinItem;
                    MySQLTable table = tableReference.getTable();
                    //从tables中移除已经连接的表
                    tables.remove(table);
                    columns.addAll(table.getColumns());
                }
                MySQLTypedExpressionGenerator joinGen = new MySQLTypedExpressionGenerator(globalState).setColumns(columns);
                MySQLExpression joinClause = joinGen.generateExpression(MySQLSchema.MySQLDataType.BOOLEAN);
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    joinClause = null;
                }

                MySQLJoin j = new MySQLJoin(joinItem, joinClause, selectedOption);
                joinStatements.add(j);
            }
        }

        return joinStatements;
    }


    private static MySQLFromItem getRandomFromItem(List<MySQLTable> tables, MySQLGlobalState globalState) {
        if (Randomly.getBoolean()) {
            // 返回一个普通表
            return new MySQLTableReference(Randomly.fromList(tables));
        } else {
            // 生成一个子查询
            return generateSubquery(tables, globalState);
        }
    }

    private static MySQLSubquery generateSubquery(List<MySQLTable> tables, MySQLGlobalState globalState) {
        MySQLSelect subquerySelect = new MySQLSelect();
        // todo
        // subquerySelect.setFromList(...);
        // subquerySelect.setSelectItems(...);
        // subquerySelect.setWhereClause(...);
        return new MySQLSubquery(subquerySelect);
    }
}
