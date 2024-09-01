package sqlancer.mysql.gen;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLCommon;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.ast.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class MySQLRandomQuerySynthesizer {

    private MySQLRandomQuerySynthesizer() {
    }

    public static MySQLSelect generate(MySQLGlobalState globalState, int nrColumns) {
        MySQLTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        MySQLUntypedExpressionGenerator gen = new MySQLUntypedExpressionGenerator(globalState).setColumns(tables.getColumns());
        MySQLSelect select = new MySQLSelect();
        List<MySQLExpression> columns = new ArrayList<>();

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        columns.addAll(gen.generateExpressions(nrColumns));
        select.setFetchColumns(columns);
        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return select;
    }

    public static MySQLSelect generateTyped(MySQLGlobalState globalState, int nrColumns) {
        MySQLTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        MySQLTypedExpressionGenerator gen = new MySQLTypedExpressionGenerator(globalState).setColumns(tables.getColumns());
        MySQLSelect select = new MySQLSelect();

        boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<MySQLExpression> columns = new ArrayList<>();
        List<MySQLExpression> columnsWithoutAggregates = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // TODO

//            if (allowAggregates && Randomly.getBoolean()) {
                MySQLExpression expression = gen.generateExpression(MySQLSchema.MySQLDataType.getRandom(globalState));
                columns.add(expression);
                columnsWithoutAggregates.add(expression);
//            }
//            else {
//                columns.add(gen.generateAggregate());
//            }
        }
        select.setFetchColumns(columns);
        List<MySQLTableReference> tableList = tables.getTables().stream()
                .map(MySQLTableReference::new).collect(Collectors.toList());
        List<MySQLExpression> updatedTableList = MySQLCommon.getTableReferences(tableList);

        // TODO: support join later
//        if (Randomly.getBoolean()) {
//            select.setJoinList(MySQLJoin.getRandomJoinClauses(tables.getTables(), globalState));
//        }

        select.setFromList(updatedTableList);
//        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(MySQLSchema.MySQLDataType.BOOLEAN));
//        }

        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }

//        if (Randomly.getBoolean()) {
//            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
//            if (Randomly.getBoolean()) {
//                select.setHavingClause(gen.generateHavingClause());
//            }
//        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return select;
    }

    public static MySQLSelect generateTypedSingleColumn(MySQLGlobalState globalState, MySQLSchema.MySQLDataType requiredType) {
        MySQLTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        MySQLTypedExpressionGenerator gen = new MySQLTypedExpressionGenerator(globalState).setColumns(tables.getColumns());
        MySQLSelect select = new MySQLSelect();

        // Generate a single column of the required type
        MySQLExpression column = gen.generateExpression(requiredType);
        select.setFetchColumns(List.of(column));

        List<MySQLTableReference> tableList = tables.getTables().stream()
                .map(MySQLTableReference::new).collect(Collectors.toList());
        List<MySQLExpression> updatedTableList = MySQLCommon.getTableReferences(tableList);
        select.setFromList(updatedTableList);

        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(MySQLSchema.MySQLDataType.BOOLEAN));
        }

        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }

        return select;
    }

}
