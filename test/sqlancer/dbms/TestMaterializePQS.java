package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestMaterializePQS {

    @Test
    public void test() {
        assumeTrue(TestConfig.isEnvironmentTrue(TestConfig.MATERIALIZE_ENV));
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "--random-string-generation",
                        "ALPHANUMERIC_SPECIALCHAR", "--username", "materialize", "materialize", "--oracle", "pqs",
                        "--set-max-tables-mvs", "true" }));
    }

}
