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

import static sqlancer.mysql.gen.MySQLTypedExpressionGenerator.generateJoin;

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

    public static MySQLSelect generateTyped(MySQLGlobalState globalState, int nrColumns, MySQLSchema.MySQLDataType requiredType, boolean allowAgg, boolean addSkipAndLimit) {
        MySQLSelect select = new MySQLSelect();
        // Choose a join or raw tables
        MySQLTables tables;
        List<MySQLSchema.MySQLEdge> edges = new ArrayList<>();
        if (Randomly.getBoolean()) {
            edges = new ArrayList<>(globalState.getSchema().getRandomConnectedEdges());
        } else {
            MySQLSchema.MySQLEdge edge = globalState.getSchema().getRandomEdge();
            if(edge != null) {
                edges.add(edge);
            }
        }
        if (Randomly.getBoolean()&&!edges.isEmpty()) {
            List<MySQLExpression> joinStatement = new ArrayList<>();
            boolean isFirstJoin = true;
            List<MySQLSchema.MySQLTable> existingTables = new ArrayList<>();//store the tables that have been joined
            for(MySQLSchema.MySQLEdge edge : edges) {
                joinStatement.add(generateJoin(edge, isFirstJoin,existingTables));
                if(!existingTables.contains(edge.getSourceTable())){
                    existingTables.add(edge.getSourceTable());
                }
                if(!existingTables.contains(edge.getTargetTable())){
                    existingTables.add(edge.getTargetTable());
                }
                isFirstJoin = false;
            }

            select.setJoinList(joinStatement);

            List<MySQLSchema.MySQLTable> nodeTables = new ArrayList<>();

            for (MySQLSchema.MySQLEdge edge : edges) {
                if (!nodeTables.contains(edge.getSourceTable())) {
                    nodeTables.add(edge.getSourceTable());
                }
                if (!nodeTables.contains(edge.getTargetTable())) {
                    nodeTables.add(edge.getTargetTable());
                }
            }

            tables = new MySQLTables(nodeTables);

        } else {
            tables = globalState.getSchema().getRandomTableNonEmptyTables();
        }

        MySQLTypedExpressionGenerator gen = new MySQLTypedExpressionGenerator(globalState).setColumns(tables.getColumns());

        List<MySQLTableReference> tableList = tables.getTables().stream()
                .map(MySQLTableReference::new).collect(Collectors.toList());
        List<MySQLExpression> updatedTableList = MySQLCommon.getTableReferences(tableList);

        if (select.getJoinList().isEmpty()) {
            select.setFromList(updatedTableList);
        } else {
            select.setFromList(new ArrayList<>());
        }

        List<MySQLExpression> columns = new ArrayList<>();

        // todo: change prob
        // if choose to allow aggregates, only agg is allowed after SELECT
        // doing this is to prevent false positive in differential testing
        if (allowAgg && Randomly.getBoolean()) {
            // gen aggregate
            for (int i = 0; i < nrColumns; i++) {
                columns.add(gen.generateAggregate());
            }
        } else {
            for (int i = 0; i < nrColumns; i++) {
                MySQLSchema.MySQLDataType dataType = requiredType == null ? MySQLSchema.MySQLDataType.getRandom(globalState) : requiredType;
                MySQLExpression expression = gen.generateExpression(dataType);
                columns.add(expression);
            }
        }

        select.setFetchColumns(columns);

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
        if (addSkipAndLimit) {
            if (Randomly.getBoolean()) {
                select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
                if (Randomly.getBoolean()) {
                    select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
                }
            }
        }
        return select;
    }

    public static MySQLSelect generateTypedSingleColumnWithoutSkipAndLimit(MySQLGlobalState globalState, MySQLSchema.MySQLDataType requiredType) {
        return generateTyped(globalState, 1, requiredType, true, false);
    }
}
