package sqlancer.mysql.gen;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLCommon;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.ast.*;

import java.util.*;
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

    public static List<MySQLSchema.MySQLEdge> getRandomEdgeChain(List<MySQLSchema.MySQLEdge> allEdges) {
        if (allEdges == null || allEdges.isEmpty()) {
            return Collections.emptyList();
        }

        List<MySQLSchema.MySQLEdge> resultChain = new ArrayList<>();
        Random random = new Random();

        // Create a map from sourceTable to list of edges
        Map<MySQLSchema.MySQLTable, List<MySQLSchema.MySQLEdge>> edgesBySource = new HashMap<>();
        for (MySQLSchema.MySQLEdge edge : allEdges) {
            edgesBySource.computeIfAbsent(edge.getSourceTable(), k -> new ArrayList<>()).add(edge);
        }

        // Randomly select a starting edge
        MySQLSchema.MySQLEdge currentEdge = allEdges.get(random.nextInt(allEdges.size()));
        resultChain.add(currentEdge);

        // Remove the starting edge from the available edges
        edgesBySource.get(currentEdge.getSourceTable()).remove(currentEdge);

        MySQLSchema.MySQLTable currentTargetTable = currentEdge.getTargetTable();

        while (true) {
            // Find all edges where sourceTable == currentTargetTable
            List<MySQLSchema.MySQLEdge> nextEdges = edgesBySource.get(currentTargetTable);
            if (nextEdges == null || nextEdges.isEmpty() || Randomly.getBoolean()) {
                // No more edges to extend the chain
                break;
            }

            // Randomly select one of these edges
            MySQLSchema.MySQLEdge nextEdge = nextEdges.get(random.nextInt(nextEdges.size()));
            resultChain.add(nextEdge);

            // Remove the selected edge from the list to prevent reusing it
            nextEdges.remove(nextEdge);
            if (nextEdges.isEmpty()) {
                edgesBySource.remove(currentTargetTable);
            }

            // Update currentTargetTable
            currentTargetTable = nextEdge.getTargetTable();
        }

        return resultChain;
    }

    public static List<MySQLExpression> createJoinsFromEdges(List<MySQLSchema.MySQLEdge> allEdges) {
        // Get a random chain of connected edges
        List<MySQLSchema.MySQLEdge> edgeChain = getRandomEdgeChain(allEdges);
        List<MySQLExpression> joins = new ArrayList<>();
        boolean isFirstJoin = true;

        for (MySQLSchema.MySQLEdge edge : edgeChain) {
            MySQLSchema.MySQLTable leftTable = edge.getSourceTable();
            MySQLSchema.MySQLTable rightTable = edge.getTargetTable();
            MySQLSchema.MySQLColumn leftColumn = edge.getSourceColumn();
            MySQLSchema.MySQLColumn rightColumn = edge.getTargetColumn();

            // todo: add left/right/full [outer] join
            MySQLJoin.JoinType joinType = Randomly.fromOptions(MySQLJoin.JoinType.INNER);

            MySQLExpression onClause = null;
            if (joinType != MySQLJoin.JoinType.NATURAL) {
                onClause = new MySQLBinaryComparisonOperation(
                        new MySQLColumnReference(leftColumn, null),
                        new MySQLColumnReference(rightColumn, null),
                        MySQLBinaryComparisonOperation.BinaryComparisonOperator.EQUALS
                );
            }
            MySQLJoin join = new MySQLJoin(leftTable, rightTable, onClause, joinType, isFirstJoin);
            joins.add(join);

            // After the first join, set isFirstJoin to false
            isFirstJoin = false;
        }

        return joins;
    }

    public static MySQLSelect generateTyped(MySQLGlobalState globalState, int nrColumns, MySQLSchema.MySQLDataType requiredType, boolean allowAgg, boolean addSkipAndLimit, boolean allowNull) {
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
            List<MySQLExpression> joinStatement = createJoinsFromEdges(edges);
//            boolean isFirstJoin = true;
//            List<MySQLSchema.MySQLTable> existingTables = new ArrayList<>();//store the tables that have been joined
//            for(MySQLSchema.MySQLEdge edge : edges) {
//                joinStatement.add(generateJoin(edge, isFirstJoin,existingTables));
//                if(!existingTables.contains(edge.getSourceTable())){
//                    existingTables.add(edge.getSourceTable());
//                }
//                if(!existingTables.contains(edge.getTargetTable())){
//                    existingTables.add(edge.getTargetTable());
//                }
//                isFirstJoin = false;
//            }

            select.setJoinList(joinStatement);

            List<MySQLSchema.MySQLTable> nodeTables = new ArrayList<>();

             for (MySQLExpression e : joinStatement) {
                var join = (MySQLJoin) e;
                 if (!nodeTables.contains(join.getLeftTable())) {
                     nodeTables.add(join.getLeftTable());
                 }
                 if (!nodeTables.contains(join.getRightTable())) {
                     nodeTables.add(join.getRightTable());
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

        gen.setAllowSubqueries(false);

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

        gen.setAllowSubqueries(true);
        var where = gen.generateExpression(MySQLSchema.MySQLDataType.BOOLEAN);
        if (nrColumns == 1 && !allowAgg && !allowNull) {
            var col = columns.get(0);
            var colNotNull = new MySQLUnaryPostfixOperation(col, MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, true);
            where = new MySQLBinaryLogicalOperation(where, colNotNull, MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator.AND);
        }

        select.setWhereClause(where);

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
        return generateTyped(globalState, 1, requiredType, false, false, false);
    }
}
