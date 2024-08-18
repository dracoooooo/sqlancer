package sqlancer.mysql.ast;

import sqlancer.Randomly;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MySQLBinaryArithmeticOperation implements MySQLExpression {
    public enum BinaryArithmeticOperator {
        ADD("+"),
        MIN("-"),
        MUL("*"),
        FLOAT_DIV("/");
        // Currently jooq does not support `DIV`, so temporarily commented out
//        INT_DIV("DIV");
        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryArithmeticOperator getRandomFloatOp() {
//            var values = Arrays.stream(values()).filter(op -> op != INT_DIV).collect(Collectors.toList());
//            return Randomly.fromList(values);
            return Randomly.fromOptions(values());
        }

        public static BinaryArithmeticOperator getRandomIntOp() {
            var values = Arrays.stream(values()).filter(op -> op != FLOAT_DIV).collect(Collectors.toList());
            return Randomly.fromList(values);
        }
    }

    private final MySQLExpression left;
    private final MySQLExpression right;
    private final BinaryArithmeticOperator op;

    public MySQLBinaryArithmeticOperation(MySQLExpression left, MySQLExpression right, BinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public BinaryArithmeticOperator getOp() {
        return op;
    }

    public MySQLExpression getRight() {
        return right;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }
}
