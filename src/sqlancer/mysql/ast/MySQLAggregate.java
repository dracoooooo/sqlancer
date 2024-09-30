package sqlancer.mysql.ast;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLSchema;

public class MySQLAggregate implements MySQLExpression {
    public enum MySQLAggregateFunction {
        COUNT, SUM, AVG, MIN, MAX;

        public static MySQLAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    private final MySQLAggregateFunction function;
    private final MySQLExpression expr;
    private final boolean isCountStar;

    public MySQLAggregate(MySQLAggregateFunction function, MySQLExpression expr, boolean isCountStar) {
        this.function = function;
        this.expr = expr;
        this.isCountStar = isCountStar;
    }


    public MySQLSchema.MySQLDataType getType() {
        switch (function) {
            case COUNT:
                return MySQLSchema.MySQLDataType.INT;
            case AVG:
            case SUM:
                return MySQLSchema.MySQLDataType.DECIMAL;
            case MIN:
            case MAX:
                return expr.getExpectedValue().getType();
            default:
                throw new AssertionError("Unhandled aggregate function: " + function);
        }
    }

    public MySQLAggregateFunction getFunction() {
        return function;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public boolean isCountStar() {
        return isCountStar;
    }
}