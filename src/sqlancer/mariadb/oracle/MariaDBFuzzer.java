package sqlancer.mariadb.oracle;

import com.beust.jcommander.JCommander;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;


import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLProvider;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.gen.MySQLRandomQuerySynthesizer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MariaDBFuzzer implements TestOracle<MariaDBProvider.MariaDBGlobalState> {
    private MySQLGlobalState mysqlGlobalState;
    private final MariaDBProvider.MariaDBGlobalState globalState;

    public MariaDBFuzzer(MariaDBProvider.MariaDBGlobalState globalState) {
        this.globalState = globalState;
    }
    @Override
    public void check() throws Exception {
        setUpMySQLConnection();

        String mysqlQuery = MySQLVisitor.asString(MySQLRandomQuerySynthesizer.generateTyped(mysqlGlobalState, Randomly.smallNumber() + 1, true))
                + ';';
        String mariaDBQuery = convertToMariaDB(mysqlQuery);
        try {
            globalState.getLogger().writeCurrent(mariaDBQuery);
//            globalState.executeStatement(new SQLQueryAdapter(s));
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {

        }


    }

    public void setUpMySQLConnection() {
        String mysqlUsername = "root";
        String mysqlPassword = "password";
        String mysqlHost = "localhost";
        int mysqlPort = 3306;
        String mysqlUrl = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                mysqlHost, mysqlPort);
        Connection mysqlCon = null;
        try {
            mysqlCon = DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        } catch (SQLException e) {
        }
        mysqlGlobalState = new MySQLGlobalState();
        mysqlGlobalState.setConnection(new SQLConnection(mysqlCon));
        mysqlGlobalState.setDatabaseName("test");
        try {
            mysqlGlobalState.updateSchema();
        } catch (Exception e) {
        }
        MainOptions mysqlOptions = new MainOptions();
        JCommander.Builder commandBuilder = JCommander.newBuilder().addObject(mysqlOptions);
        mysqlGlobalState.setMainOptions(mysqlOptions);
        mysqlGlobalState.setStateLogger(new Main.StateLogger(mysqlGlobalState.getDatabaseName(), new MySQLProvider(), mysqlOptions));
        Main.QueryManager globalManager = new Main.QueryManager(mysqlGlobalState);
        mysqlGlobalState.setManager(globalManager);
    }

    private String convertToMariaDB(String mysqlQuery) {
        try {
            DSLContext mysqlContext = DSL.using(SQLDialect.MYSQL);
            DSLContext mariaDBContext = DSL.using(SQLDialect.MARIADB);

            org.jooq.Query parsedQuery = mysqlContext.parser().parseQuery(mysqlQuery);
            String mariaDBQuery = mariaDBContext.render(parsedQuery);

            return mariaDBQuery;

        } catch (Exception e) {
            System.err.println("Query conversion failed: " + e.getMessage());
            return null;
        }
    }
}
