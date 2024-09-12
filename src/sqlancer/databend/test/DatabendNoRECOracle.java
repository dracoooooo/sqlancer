package sqlancer.databend.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.gen.DatabendNewExpressionGenerator;

public class DatabendNoRECOracle implements TestOracle<DatabendGlobalState> {

    NoRECOracle<DatabendSelect, DatabendJoin, DatabendExpression, DatabendSchema, DatabendTable, DatabendColumn, DatabendGlobalState> oracle;

    public DatabendNoRECOracle(DatabendGlobalState globalState) {
        DatabendNewExpressionGenerator gen = new DatabendNewExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(DatabendErrors.getExpressionErrors())
                .with("canceling statement due to statement timeout").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<DatabendGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

}
