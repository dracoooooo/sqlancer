package sqlancer.mysql.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.mysql.MySQLBugs;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLTypedExpressionGenerator extends TypedExpressionGenerator<MySQLExpression, MySQLSchema.MySQLColumn, MySQLSchema.MySQLDataType> {
    private final MySQLGlobalState globalState;

    public MySQLTypedExpressionGenerator(MySQLGlobalState globalState) {
        this.globalState = globalState;
        allowAggregates = true;
    }

    @Override
    public MySQLExpression generatePredicate() {
        return generateExpression(MySQLSchema.MySQLDataType.BOOLEAN);
    }

    @Override
    public MySQLExpression negatePredicate(MySQLExpression predicate) {
        return new MySQLUnaryPrefixOperation(predicate, MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.NOT);
    }

    @Override
    public MySQLExpression isNull(MySQLExpression expr) {
        return new MySQLUnaryPostfixOperation(expr, MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public MySQLExpression generateConstant(MySQLSchema.MySQLDataType type) {
        // TODO: Support null later
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            return MySQLConstant.createNullConstant();
//        }
        switch (type) {
            case BOOLEAN:
                return MySQLConstant.createBoolean(Randomly.getBoolean());
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return MySQLConstant.createDoubleConstant(globalState.getRandomly().getDouble());
            case INT:
                return MySQLConstant.createIntConstant(globalState.getRandomly().getInteger());
            case VARCHAR:
                /* Replace characters that still trigger open bugs in MySQL */
                String string = globalState.getRandomly().getString().replace("\\", "").replace("\n", "");
                return MySQLConstant.createStringConstant(string);
            default:
                throw new AssertionError();
        }
    }

    private enum BooleanExpression {
        NOT, IS_NULL, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, BETWEEN_OPERATOR, EXISTS,
        SUBQUERY_COMPARISON_OPERATION
        // TODO
//        COMPUTABLE_FUNCTION, CAST, IN_OPERATION, BINARY_OPERATION
    }

    private enum MySQLFunction {
        CHAR_LENGTH("CHAR_LENGTH", 1, MySQLSchema.MySQLDataType.INT),
        LEFT("LEFT", 2, MySQLSchema.MySQLDataType.VARCHAR),
        LOWER("LOWER", 1, MySQLSchema.MySQLDataType.VARCHAR),
        LTRIM("LTRIM", 1, MySQLSchema.MySQLDataType.VARCHAR),
        SUBSTRING("SUBSTRING", 3, MySQLSchema.MySQLDataType.VARCHAR),
        ABS("ABS", 1, MySQLSchema.MySQLDataType.INT),
        CEILING("CEILING", 1, MySQLSchema.MySQLDataType.FLOAT),
        RAND("RAND", 0, MySQLSchema.MySQLDataType.FLOAT);

        private final String name;
        private final int argumentCount;
        private final MySQLSchema.MySQLDataType returnType;

        MySQLFunction(String name, int argumentCount, MySQLSchema.MySQLDataType returnType) {
            this.name = name;
            this.argumentCount = argumentCount;
            this.returnType = returnType;
        }
    }

    private MySQLExpression generateFunctionCall(MySQLFunction function, int depth) {
        List<MySQLExpression> args = new ArrayList<>();

        for (int i = 0; i < function.argumentCount; i++) {
            MySQLSchema.MySQLDataType argType = getArgumentType(function, i);
            args.add(generateExpression(argType, depth + 1));
        }

        return new MySQLFunctionCall(function.name, args);
    }

    private MySQLSchema.MySQLDataType getArgumentType(MySQLFunction function, int argIndex) {
        switch (function) {
            case CHAR_LENGTH:
            case LOWER:
            case LTRIM:
                return MySQLSchema.MySQLDataType.VARCHAR;
            case LEFT:
                return argIndex == 0 ? MySQLSchema.MySQLDataType.VARCHAR : MySQLSchema.MySQLDataType.INT;
            case SUBSTRING:
                return argIndex == 0 ? MySQLSchema.MySQLDataType.VARCHAR : MySQLSchema.MySQLDataType.INT;
            case ABS:
                return MySQLSchema.MySQLDataType.INT;
            case CEILING:
                return MySQLSchema.MySQLDataType.FLOAT;
            default:
                throw new AssertionError("Unexpected function: " + function);
        }
    }


    private MySQLExpression getExists() {

        MySQLGlobalState globalState = this.globalState;
        int nrColumns = Randomly.smallNumber() + 1; // randomly generate a number between 1 and 5 of columns to select

        MySQLSelect subquery;
        subquery = MySQLRandomQuerySynthesizer.generateTyped(globalState, nrColumns, false);

        boolean isNegated = Randomly.getBoolean();
        MySQLExpression existsExpression = new MySQLExists(subquery);
        if (isNegated) {
            existsExpression = new MySQLUnaryPrefixOperation(existsExpression, MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.NOT);
        }

        return existsExpression;
//        if (Randomly.getBoolean()) {
//            return new MySQLExists(new MySQLStringExpression("SELECT 1", MySQLConstant.createTrue()));
//        } else {
//            return new MySQLExists(new MySQLStringExpression("SELECT 1 WHERE FALSE", MySQLConstant.createFalse()));
//        }

    }

    private MySQLSchema.MySQLDataType getRandomTypeForBetween() {
        var typesForBetween = List.of(MySQLSchema.MySQLDataType.INT, MySQLSchema.MySQLDataType.DOUBLE,
                MySQLSchema.MySQLDataType.DECIMAL, MySQLSchema.MySQLDataType.FLOAT);
        var columnsForBetween = columns.stream().filter(c -> typesForBetween.contains(c.getType())).collect(Collectors.toList());
        if (columnsForBetween.isEmpty() || Randomly.getBooleanWithRatherLowProbability()) {
            return Randomly.fromList(typesForBetween);
        } else {
            return Randomly.fromList(columnsForBetween).getType();
        }
    }

    private MySQLExpression generateBooleanExpression(int depth) {
        BooleanExpression exprType = Randomly.fromOptions(BooleanExpression.values());
        MySQLExpression expr;
        switch (exprType) {
            case NOT:
                MySQLExpression subExpr = generateExpression(MySQLSchema.MySQLDataType.BOOLEAN, depth + 1);
                return new MySQLUnaryPrefixOperation(subExpr, MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.NOT);
            case IS_NULL:
                return new MySQLUnaryPostfixOperation(generateExpression(getRandomType(), depth + 1),
                        MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, Randomly.getBoolean());
            case BINARY_LOGICAL_OPERATOR:
                return new MySQLBinaryLogicalOperation(generateExpression(MySQLSchema.MySQLDataType.BOOLEAN, depth + 1), generateExpression(MySQLSchema.MySQLDataType.BOOLEAN, depth + 1),
                        MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator.getRandom());
            case BINARY_COMPARISON_OPERATION: {
                // TODO: maybe unable to be parsed in Cypher
                MySQLSchema.MySQLDataType type = getRandomTypeForBetween();
                var op = MySQLBinaryComparisonOperation.BinaryComparisonOperator.getRandom();
//                if (op == MySQLBinaryComparisonOperation.BinaryComparisonOperator.LIKE) {
//                    type = MySQLSchema.MySQLDataType.VARCHAR;
//                }

                return new MySQLBinaryComparisonOperation(generateExpression(type, depth + 1), generateExpression(type, depth + 1), op);
            }


            case EXISTS:
                return getExists();
            case BETWEEN_OPERATOR: {
                if (MySQLBugs.bug99181) {
                    // TODO: there are a number of bugs that are triggered by the BETWEEN operator
                    throw new IgnoreMeException();
                }
                MySQLSchema.MySQLDataType type = getRandomTypeForBetween();
                return new MySQLBetweenOperation(generateExpression(type, depth + 1), generateExpression(type, depth + 1), generateExpression(type, depth + 1));
            }
            case SUBQUERY_COMPARISON_OPERATION:
                return generateSubqueryComparison(depth + 1);
            default:
                throw new AssertionError();
        }
    }

    private enum ArithmeticExpression {
        LEAF_NODE, UNARY_OPERATION, BINARY_ARITHMETIC_OPERATION
    }

    private MySQLExpression generateIntExpression(int depth) {
        var intExpression = Randomly.fromOptions(ArithmeticExpression.values());
        switch (intExpression) {
            case LEAF_NODE:
                return generateLeafNode(MySQLSchema.MySQLDataType.INT);
            case UNARY_OPERATION:
                return new MySQLUnaryPrefixOperation(generateExpression(MySQLSchema.MySQLDataType.INT, depth + 1),
                        Randomly.getBoolean() ? MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.PLUS
                                : MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.MINUS);
            case BINARY_ARITHMETIC_OPERATION:
                return new MySQLBinaryArithmeticOperation(generateExpression(MySQLSchema.MySQLDataType.INT, depth + 1),
                        generateExpression(MySQLSchema.MySQLDataType.INT, depth + 1),
                        MySQLBinaryArithmeticOperation.BinaryArithmeticOperator.getRandomIntOp());
            default:
                throw new AssertionError();
        }
    }

    private MySQLExpression generateDecimalExpression(MySQLSchema.MySQLDataType type, int depth) {
        if (type != MySQLSchema.MySQLDataType.DECIMAL) {
            throw new AssertionError();
        }
        var decimalExpression = Randomly.fromOptions(ArithmeticExpression.values());
        switch (decimalExpression) {
            case LEAF_NODE:
                return generateLeafNode(type);
            case UNARY_OPERATION:
                return new MySQLUnaryPrefixOperation(generateExpression(type, depth + 1),
                        Randomly.getBoolean() ? MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.PLUS
                                : MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator.MINUS);
            case BINARY_ARITHMETIC_OPERATION:
                return new MySQLBinaryArithmeticOperation(generateExpression(type, depth + 1),
                        generateExpression(type, depth + 1),
                        MySQLBinaryArithmeticOperation.BinaryArithmeticOperator.getRandomFloatOp());
            default:
                throw new AssertionError();
        }
    }


    @Override
    protected MySQLExpression generateExpression(MySQLSchema.MySQLDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode(type);
        }

        if (Randomly.getBooleanWithRatherLowProbability()) {
            ArrayList<MySQLFunction> functionsToBeChosen = new ArrayList<>(Arrays.stream(MySQLFunction.values())
                    .filter(f -> f.returnType == type)
                    .collect(Collectors.toList()));

            if (!functionsToBeChosen.isEmpty()) {
                MySQLFunction function = Randomly.fromList(functionsToBeChosen);
                return generateFunctionCall(function, depth);
            }
        }
        // BOOLEAN, INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;
        switch (type) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case INT:
                return generateIntExpression(depth);
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case VARCHAR:
                return generateLeafNode(type);
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected MySQLExpression generateColumn(MySQLSchema.MySQLDataType type) {
        MySQLSchema.MySQLColumn column = Randomly.fromList(columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList()));
        return MySQLColumnReference.create(column, null);
    }

    @Override
    protected MySQLSchema.MySQLDataType getRandomType() {
        if (columns.isEmpty() || Randomly.getBooleanWithRatherLowProbability()) {
            return MySQLSchema.MySQLDataType.getRandom(globalState);
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    @Override
    protected boolean canGenerateColumnOfType(MySQLSchema.MySQLDataType type) {
        return columns.stream().anyMatch(c -> c.getType() == type);
    }

    @Override
    public List<MySQLExpression> generateOrderBys() {
        List<MySQLExpression> expressions = super.generateOrderBys();
        List<MySQLExpression> newOrderBys = new ArrayList<>();
        for (MySQLExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                MySQLOrderByTerm newExpr = new MySQLOrderByTerm(expr, MySQLOrderByTerm.MySQLOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }


    public MySQLExpression generateAggregate() {
        return getAggregate(MySQLAggregate.MySQLAggregateFunction.getRandom());
    }

    private MySQLExpression getAggregate(MySQLAggregate.MySQLAggregateFunction function) {

        if (function == MySQLAggregate.MySQLAggregateFunction.COUNT && Randomly.getBoolean()) {
            // COUNT(*)
            return new MySQLAggregate(function, null, true);
        } else {
            switch (function) {
                case COUNT:
                    return new MySQLAggregate(function, generateExpression(MySQLSchema.MySQLDataType.getRandom(globalState), 0), false);
                case SUM:
                case AVG:
                case MIN:
                case MAX:
                    List<MySQLSchema.MySQLDataType> numericTypes = Arrays.asList(MySQLSchema.MySQLDataType.INT, MySQLSchema.MySQLDataType.FLOAT, MySQLSchema.MySQLDataType.DOUBLE, MySQLSchema.MySQLDataType.DECIMAL);
                    return new MySQLAggregate(function, generateExpression(Randomly.fromList(numericTypes), 0), false);
                default:
                    throw new AssertionError();
            }
        }
    }


        public MySQLExpression generateHavingClause () {
            allowAggregates = true;
            MySQLExpression expression = generateExpression(MySQLSchema.MySQLDataType.BOOLEAN);
            allowAggregates = false;
            return expression;
        }

        private MySQLExpression generateSubqueryComparison ( int depth){
            MySQLSchema.MySQLDataType type = MySQLSchema.MySQLDataType.getRandom(globalState);
            MySQLExpression leftExpression = generateExpression(type, depth + 1);
            MySQLBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator =
                    MySQLBinaryComparisonOperation.BinaryComparisonOperator.getRandom();
            MySQLSubqueryComparisonOperation.SubqueryComparisonOperator subqueryOperator = MySQLSubqueryComparisonOperation.SubqueryComparisonOperator.getRandom();

            // Generate a subquery that returns a single column of the same type
            MySQLSelect subquery = MySQLRandomQuerySynthesizer.generateTypedSingleColumnWithoutSkipAndLimit(globalState, type);

            return new MySQLSubqueryComparisonOperation(leftExpression, comparisonOperator, subqueryOperator, subquery);
        }

        public MySQLExpression generateJoin (MySQLGlobalState globalState){
            MySQLSchema.MySQLEdge edge = globalState.getSchema().getRandomEdge();
            MySQLSchema.MySQLTable leftTable = edge.getSourceTable();
            MySQLSchema.MySQLTable rightTable = edge.getTargetTable();
            MySQLSchema.MySQLColumn leftColumn = edge.getSourceColumn();
            MySQLSchema.MySQLColumn rightColumn = edge.getTargetColumn();

            MySQLJoin.JoinType joinType = Randomly.fromOptions(MySQLJoin.JoinType.values());

            MySQLExpression onClause = null;
            if (joinType != MySQLJoin.JoinType.NATURAL) {
                onClause = new MySQLBinaryComparisonOperation(
                        new MySQLColumnReference(leftColumn, null),
                        new MySQLColumnReference(rightColumn, null),
                        MySQLBinaryComparisonOperation.BinaryComparisonOperator.EQUALS
                );
            }

            MySQLSchema.MySQLTable joinTable = Randomly.fromOptions(leftTable, rightTable);
            return new MySQLJoin(joinTable, onClause, joinType);

        }


    }
