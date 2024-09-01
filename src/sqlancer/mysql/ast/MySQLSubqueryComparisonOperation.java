package sqlancer.mysql.ast;

import sqlancer.Randomly;

public class MySQLSubqueryComparisonOperation implements MySQLExpression {

    public enum SubqueryComparisonOperator {
        ANY, ALL;

        public static SubqueryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    private final MySQLExpression leftExpression;
    private final MySQLBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator;
    private final SubqueryComparisonOperator subqueryOperator;
    private final MySQLSelect subquery;

    public MySQLSubqueryComparisonOperation(MySQLExpression leftExpression,
                                            MySQLBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator,
                                            SubqueryComparisonOperator subqueryOperator,
                                            MySQLSelect subquery) {
        this.leftExpression = leftExpression;
        this.comparisonOperator = comparisonOperator;
        this.subqueryOperator = subqueryOperator;
        this.subquery = subquery;
    }

    public MySQLExpression getLeftExpression() {
        return leftExpression;
    }

    public MySQLBinaryComparisonOperation.BinaryComparisonOperator getComparisonOperator() {
        return comparisonOperator;
    }

    public SubqueryComparisonOperator getSubqueryOperator() {
        return subqueryOperator;
    }

    public MySQLSelect getSubquery() {
        return subquery;
    }
}
