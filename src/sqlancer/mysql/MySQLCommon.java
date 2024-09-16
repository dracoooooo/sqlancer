package sqlancer.mysql;

import sqlancer.Randomly;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLTableReference;

import java.math.BigDecimal;
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

    public static MySQLExpression convertObjectToExpression(Object o, MySQLSchema.MySQLDataType type) {
        switch (type) {
            case VARCHAR:
                return new MySQLConstant.MySQLTextConstant((String)o);
            case INT:
                return new MySQLConstant.MySQLIntConstant((int)o, String.valueOf(o));
            case BOOLEAN:
                return new MySQLConstant.MySQLBooleanConstant((boolean)o);
            case FLOAT:
                return new MySQLConstant.MySQLDoubleConstant((float)o);
            case DOUBLE:
                return new MySQLConstant.MySQLDoubleConstant((double)o);
            case DECIMAL:
                return new MySQLConstant.MySQLTextConstant(((BigDecimal)o).toString());
            default:
                throw new AssertionError(type);
        }
    }



}
