package sqlancer.mysql.gen;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.mysql.cj.exceptions.AssertionFailedException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.*;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;

public class MySQLInsertGenerator {

    private final MySQLTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final MySQLGlobalState globalState;

    public MySQLInsertGenerator(MySQLGlobalState globalState, MySQLTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter insertRow(MySQLGlobalState globalState) throws SQLException {
        MySQLTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(MySQLGlobalState globalState, MySQLTable table) throws SQLException {
        if (Randomly.getBoolean()) {
            return new MySQLInsertGenerator(globalState, table).generateInsert();
        } else {
            return new MySQLInsertGenerator(globalState, table).generateReplace();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
        }
        return generateInto();

    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        return generateInto();
    }

    private SQLQueryAdapter generateInto() {
        sb.append(" INTO ");
        sb.append(table.getName());
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");
        MySQLTypedExpressionGenerator gen = new MySQLTypedExpressionGenerator(globalState);
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int c = 0; c < columns.size(); c++) {
                if (c != 0) {
                    sb.append(", ");
                }
                var col = columns.get(c);
                if (col.isForeignKey()) {
                    // Select a random value from the referenced table if the col is a foreign key
                    sb.append(MySQLVisitor.asString(getRandomFromPrimaryKey(col.getRefColumn())));
                } else {
                    sb.append(MySQLVisitor.asString(gen.generateConstant(columns.get(c).getType())));
                }

            }
            sb.append(")");
        }
        MySQLErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private MySQLExpression getRandomFromPrimaryKey(MySQLColumn column) {
        if (!column.isPrimaryKey()) {
            throw new AssertionError();
        }
        var tableName = column.getTable().getName();
        var primaryKey = column.getName();

        var con = globalState.getConnection();
        try (Statement s = con.createStatement()) {
            // SELECT a random primary row from the table
            try (ResultSet rs = s.executeQuery(String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", primaryKey, tableName))) {
                if (rs.next()) {
                    var val = rs.getObject(primaryKey);
                    return MySQLCommon.convertObjectToExpression(val, column.getType());
                } else {
                    return new MySQLConstant.MySQLNullConstant();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
