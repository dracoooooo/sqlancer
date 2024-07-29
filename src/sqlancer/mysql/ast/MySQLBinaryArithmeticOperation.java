package sqlancer.mysql.ast;

import sqlancer.Randomly;

public class MySQLBinaryArithmeticOperation implements MySQLExpression {
    public enum BinaryArithmeticOperator {
        ADD("+"),
        MIN("-"),
        MUL("*"),
        DIV("/");
        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
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
