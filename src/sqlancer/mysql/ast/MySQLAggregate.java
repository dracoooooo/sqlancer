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


    public MySQLExpression getExpr() {
        return expr;
    }

    public String getFunctionName() {
        switch (function)
        {
            case COUNT:
                return "COUNT";
            case SUM:
                return "SUM";
            case AVG:
                return "AVG";
            case MIN:
                return "MIN";
            case MAX:
                return "MAX";
            default:
                throw new AssertionError();
        }
    }

    public boolean isCountStar() {
        return isCountStar;
    }


}