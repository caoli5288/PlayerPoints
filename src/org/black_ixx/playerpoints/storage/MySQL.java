package org.black_ixx.playerpoints.storage;

import lib.PatPeter.SQLibrary.DBMS;
import lib.PatPeter.SQLibrary.HostnameDatabase;
import lib.PatPeter.SQLibrary.StatementEnum;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Created on 17-2-20.
 */
public class MySQL extends HostnameDatabase {
    /**
     * @deprecated
     */
    @Deprecated
    public MySQL(Logger log, String prefix, String hostname, String port, String database, String username, String password) {
        super(log, prefix, DBMS.MySQL, hostname, Integer.parseInt(port), database, username, password);
    }

    public MySQL(Logger log, String prefix, String hostname, int port, String database, String username, String password) {
        super(log, prefix, DBMS.MySQL, hostname, port, database, username, password);
    }

    public MySQL(Logger log, String prefix, String database, String username, String password) {
        super(log, prefix, DBMS.MySQL, "localhost", 3306, database, username, password);
    }

    public MySQL(Logger log, String prefix, String database, String username) {
        super(log, prefix, DBMS.MySQL, "localhost", 3306, database, username, "");
    }

    public MySQL(Logger log, String prefix, String database) {
        super(log, prefix, DBMS.MySQL, "localhost", 3306, database, "", "");
    }

    protected boolean initialize() {
        try {
            Class.forName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
            return true;
        } catch (ClassNotFoundException var2) {
            this.warning("MySQL DataSource class missing: " + var2.getMessage() + ".");
            return false;
        }
    }

    public boolean open() {
        try {
            String e = "jdbc:mysql://" + this.getHostname() + ":" + this.getPort() + "/" + this.getDatabase();
            if (this.initialize()) {
                this.connection = DriverManager.getConnection(e, this.getUsername(), this.getPassword());
                return true;
            } else {
                return false;
            }
        } catch (SQLException var2) {
            this.error("Could not establish a MySQL connection, SQLException: " + var2.getMessage());
            return false;
        }
    }

    protected void queryValidation(StatementEnum statement) throws SQLException {
        switch (((Statements) statement).ordinal()) {
            case 1:
                this.warning("Please create a new connection to use a different database.");
                throw new SQLException("Please create a new connection to use a different database.");
            case 2:
            case 3:
            case 4:
                this.warning("Please use the prepare() method to prepare a query.");
                throw new SQLException("Please use the prepare() method to prepare a query.");
            default:
        }
    }

    public lib.PatPeter.SQLibrary.MySQL.Statements getStatement(String query) throws SQLException {
        String[] statement = query.trim().split(" ", 2);

        try {
            lib.PatPeter.SQLibrary.MySQL.Statements e = lib.PatPeter.SQLibrary.MySQL.Statements.valueOf(statement[0].toUpperCase());
            return e;
        } catch (IllegalArgumentException var4) {
            throw new SQLException("Unknown statement: \"" + statement[0] + "\".");
        }
    }

    /**
     * @deprecated
     */
    @Deprecated
    public boolean createTable(String query) {
        Statement statement = null;
        if (query != null && !query.equals("")) {
            try {
                statement = this.connection.createStatement();
                statement.execute(query);
                statement.close();
                return true;
            } catch (SQLException var4) {
                this.writeError("Could not create table, SQLException: " + var4.getMessage(), true);
                return false;
            }
        } else {
            this.writeError("Could not create table: query is empty or null.", true);
            return false;
        }
    }

    public boolean isTable(String table) {
        Statement statement;
        try {
            statement = this.connection.createStatement();
        } catch (SQLException var5) {
            this.error("Could not create a statement in checkTable(), SQLException: " + var5.getMessage());
            return false;
        }

        try {
            statement.executeQuery("SELECT 1 FROM " + table);
            return true;
        } catch (SQLException var4) {
            return false;
        }
    }

    public boolean truncate(String table) {
        Statement statement = null;
        String query = null;

        try {
            if (!this.isTable(table)) {
                this.error("Table \"" + table + "\" does not exist.");
                return false;
            } else {
                statement = this.connection.createStatement();
                query = "DELETE FROM " + table + ";";
                statement.executeUpdate(query);
                statement.close();
                return true;
            }
        } catch (SQLException var5) {
            this.error("Could not wipe table, SQLException: " + var5.getMessage());
            return false;
        }
    }

    public static enum Statements implements StatementEnum {
        SELECT("SELECT"),
        INSERT("INSERT"),
        UPDATE("UPDATE"),
        DELETE("DELETE"),
        DO("DO"),
        REPLACE("REPLACE"),
        LOAD("LOAD"),
        HANDLER("HANDLER"),
        CALL("CALL"),
        CREATE("CREATE"),
        ALTER("ALTER"),
        DROP("DROP"),
        TRUNCATE("TRUNCATE"),
        RENAME("RENAME"),
        START("START"),
        COMMIT("COMMIT"),
        SAVEPOINT("SAVEPOINT"),
        ROLLBACK("ROLLBACK"),
        RELEASE("RELEASE"),
        LOCK("LOCK"),
        UNLOCK("UNLOCK"),
        PREPARE("PREPARE"),
        EXECUTE("EXECUTE"),
        DEALLOCATE("DEALLOCATE"),
        SET("SET"),
        SHOW("SHOW"),
        DESCRIBE("DESCRIBE"),
        EXPLAIN("EXPLAIN"),
        HELP("HELP"),
        USE("USE");

        private String string;

        private Statements(String string) {
            this.string = string;
        }

        public String toString() {
            return this.string;
        }
    }
}
