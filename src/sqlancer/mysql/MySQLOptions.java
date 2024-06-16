package sqlancer.mysql;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;
import sqlancer.mysql.oracle.MySQLFuzzer;

import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "MySQL (default port: " + MySQLOptions.DEFAULT_PORT
        + ", default host: " + MySQLOptions.DEFAULT_HOST + ")")
public class MySQLOptions implements DBMSSpecificOptions<MySQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MySQLOracleFactory> oracles = Arrays.asList(MySQLOracleFactory.FUZZER);

    public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {
        FUZZER {
            @Override
            public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws Exception {
                return new MySQLFuzzer(globalState);
            }

        },
    }

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
