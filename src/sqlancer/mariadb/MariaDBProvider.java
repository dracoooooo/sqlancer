package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.gen.MariaDBIndexGenerator;
import sqlancer.mariadb.gen.MariaDBInsertGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;
import sqlancer.mariadb.gen.MariaDBTableAdminCommandGenerator;
import sqlancer.mariadb.gen.MariaDBTableGenerator;
import sqlancer.mariadb.gen.MariaDBTruncateGenerator;
import sqlancer.mariadb.gen.MariaDBUpdateGenerator;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLProvider;

@AutoService(DatabaseProvider.class)
public class MariaDBProvider extends SQLProviderAdapter<MariaDBGlobalState, MariaDBOptions> {

    public static final int MAX_EXPRESSION_DEPTH = 3;

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    enum Action {
        ANALYZE_TABLE, //
        CHECKSUM, //
        CHECK_TABLE, //
        CREATE_INDEX, //
        INSERT, //
        OPTIMIZE, //
        REPAIR_TABLE, //
        SET, //
        TRUNCATE, //
        UPDATE, //
    }
    public void MySQLCreateAndGenerateDatabase(MySQLGlobalState globalState) throws SQLException {
        // Create database
        String username = "root";
        String password = "password";
        String host = "localhost";
        int port = 3306;

        MainOptions mysqlOptions = new MainOptions();
        JCommander.Builder commandBuilder = JCommander.newBuilder().addObject(mysqlOptions);
        globalState.setMainOptions(mysqlOptions);
        globalState.setStateLogger(new Main.StateLogger(globalState.getDatabaseName(), new MySQLProvider(), mysqlOptions));
        Main.QueryManager globalManager = new Main.QueryManager(globalState);
        globalState.setManager(globalManager);
        String databaseName = globalState.getDatabaseName();

        globalState.setState(new StateToReproduce(databaseName, this));
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        globalState.setConnection(new SQLConnection(con));


        // Generate database
        MySQLProvider provider = new MySQLProvider();
        try {
            provider.generateDatabase(globalState);
        } catch (Exception e) {
            throw new RuntimeException("Cannot generate database", e);
        }
    }

    @Override
    public void generateDatabase(MariaDBGlobalState globalState) throws Exception {

        MySQLGlobalState mysqlGlobalState = new MySQLGlobalState();
        MySQLCreateAndGenerateDatabase(mysqlGlobalState);

        String mariadbUsername = globalState.getOptions().getUserName();
        String mariadbPassword = globalState.getOptions().getPassword();
        String mariadbHost = globalState.getOptions().getHost();
        int mariadbPort = globalState.getOptions().getPort();
        if (mariadbHost == null) {
            mariadbHost = MariaDBOptions.DEFAULT_HOST;
        }
        if (mariadbPort == MainOptions.NO_SET_PORT) {
            mariadbPort = MariaDBOptions.DEFAULT_PORT;
        }
        String mariadbUrl = String.format("jdbc:mariadb://%s:%d", mariadbHost, mariadbPort);
        Connection mariadbCon = DriverManager.getConnection(mariadbUrl, mariadbUsername, mariadbPassword);

        String mysqlUsername = "root";
        String mysqlPassword = "password";
        String mysqlHost = "localhost";
        int mysqlPort = 3306;
        String mysqlUrl = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                mysqlHost, mysqlPort);
        Connection mysqlCon = DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);

        // Create tables in MariaDB
        MySQLToMariaDBMigrator.migrateSchema(mysqlCon, mariadbCon, "test",globalState);

        MainOptions options = globalState.getOptions();


//        int[] nrRemaining = new int[Action.values().length];
//        List<Action> actions = new ArrayList<>();
//        int total = 0;
//        for (int i = 0; i < Action.values().length; i++) {
//            Action action = Action.values()[i];
//            int nrPerformed = 0;
//            switch (action) {
//            case CHECKSUM:
//            case CHECK_TABLE:
//            case TRUNCATE:
//            case REPAIR_TABLE:
//            case OPTIMIZE:
//            case ANALYZE_TABLE:
//            case UPDATE:
//            case CREATE_INDEX:
//                nrPerformed = globalState.getRandomly().getInteger(0, 2);
//                break;
//            case SET:
//                nrPerformed = 20;
//                break;
//            case INSERT:
//                nrPerformed = globalState.getRandomly().getInteger(0, options.getMaxNumberInserts());
//                break;
//            default:
//                throw new AssertionError(action);
//            }
//            if (nrPerformed != 0) {
//                actions.add(action);
//            }
//            nrRemaining[action.ordinal()] = nrPerformed;
//            total += nrPerformed;
//        }
//        while (total != 0) {
//            Action nextAction = null;
//            int selection = globalState.getRandomly().getInteger(0, total);
//            int previousRange = 0;
//            for (int i = 0; i < nrRemaining.length; i++) {
//                if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
//                    nextAction = Action.values()[i];
//                    break;
//                } else {
//                    previousRange += nrRemaining[i];
//                }
//            }
//            assert nextAction != null;
//            assert nrRemaining[nextAction.ordinal()] > 0;
//            nrRemaining[nextAction.ordinal()]--;
//            SQLQueryAdapter query;
//            try {
//                switch (nextAction) {
//                case CHECKSUM:
//                    query = MariaDBTableAdminCommandGenerator.checksumTable(globalState.getSchema());
//                    break;
//                case CHECK_TABLE:
//                    query = MariaDBTableAdminCommandGenerator.checkTable(globalState.getSchema());
//                    break;
//                case TRUNCATE:
//                    query = MariaDBTruncateGenerator.truncate(globalState.getSchema());
//                    break;
//                case REPAIR_TABLE:
//                    query = MariaDBTableAdminCommandGenerator.repairTable(globalState.getSchema());
//                    break;
//                case INSERT:
//                    query = MariaDBInsertGenerator.insert(globalState.getSchema(), globalState.getRandomly());
//                    break;
//                case OPTIMIZE:
//                    query = MariaDBTableAdminCommandGenerator.optimizeTable(globalState.getSchema());
//                    break;
//                case ANALYZE_TABLE:
//                    query = MariaDBTableAdminCommandGenerator.analyzeTable(globalState.getSchema());
//                    break;
//                case UPDATE:
//                    query = MariaDBUpdateGenerator.update(globalState.getSchema(), globalState.getRandomly());
//                    break;
//                case CREATE_INDEX:
//                    query = MariaDBIndexGenerator.generate(globalState.getSchema());
//                    break;
//                case SET:
//                    query = MariaDBSetGenerator.set(globalState.getRandomly(), options);
//                    break;
//                default:
//                    throw new AssertionError(nextAction);
//                }
//            } catch (IgnoreMeException e) {
//                total--;
//                continue;
//            }
//            try {
//                globalState.executeStatement(query);
//            } catch (Throwable t) {
//                System.err.println(query.getQueryString());
//                throw t;
//            }
//            total--;
//        }
    }

    public static class MariaDBGlobalState extends SQLGlobalState<MariaDBOptions, MariaDBSchema> {

        @Override
        protected MariaDBSchema readSchema() throws SQLException {
            return MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public SQLConnection createDatabase(MariaDBGlobalState globalState) throws SQLException {
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        globalState.getState().logStatement("CREATE DATABASE " + globalState.getDatabaseName());
        globalState.getState().logStatement("USE " + globalState.getDatabaseName());
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MariaDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MariaDBOptions.DEFAULT_PORT;
        }
        String url = String.format("jdbc:mariadb://%s:%d", host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + globalState.getDatabaseName());
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

}
