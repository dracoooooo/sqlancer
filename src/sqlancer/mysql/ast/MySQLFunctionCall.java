package sqlancer.mysql.ast;

import java.util.List;

public class MySQLFunctionCall implements MySQLExpression {
    private final String functionName;
    private final List<MySQLExpression> arguments;

    public MySQLFunctionCall(String functionName, List<MySQLExpression> arguments) {
        this.functionName = functionName;
        this.arguments = arguments;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<MySQLExpression> getArguments() {
        return arguments;
    }

}