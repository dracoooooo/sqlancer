package sqlancer.mysql;

import sqlancer.Randomly;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLTableReference;

import java.util.ArrayList;
import java.util.List;

public final class MySQLCommon {

    private MySQLCommon() {
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("en", "de", "es", "cmn");
    }

    public static List<MySQLExpression> getTableReferences(List<MySQLTableReference> tableList) {
        List<MySQLExpression> from = new ArrayList<>();
        for (MySQLTableReference t : tableList) {
            MySQLSchema.MySQLTable table = t.getTable();
            // TODO
//            if (!table.getIndexes().isEmpty() && Randomly.getBooleanWithSmallProbability()) {
//                from.add(new CockroachDBIndexReference(t, table.getRandomIndex()));
//            } else {
                from.add(t);
//            }
        }
        return from;
    }



}
