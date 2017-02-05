package cspro2sql;

import cspro2sql.bean.DictionaryInfo;
import cspro2sql.sql.DictionaryQuery;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author Istat Cooperation Unit
 */
public class StatusEngine {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try (InputStream in = StatusEngine.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            return;
        }
        try {
            execute(prop);
        } catch (Exception ex) {
            System.exit(1);
        }
    }

    static boolean execute(Properties prop) {
        boolean notRunning = true;
        String srcDataTable = prop.getProperty("db.source.data.table");

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();

            //Connect to the destination database
            try (Connection connDst = DriverManager.getConnection(
                    prop.getProperty("db.dest.uri") + "/" + prop.getProperty("db.dest.schema") + "?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.dest.username"),
                    prop.getProperty("db.dest.password"))) {
                connDst.setReadOnly(true);

                DictionaryQuery dictionaryQuery = new DictionaryQuery(connDst);
                DictionaryInfo dictionaryInfo = dictionaryQuery.getDictionaryInfo(srcDataTable);
                dictionaryInfo.print(System.out);
                notRunning = !dictionaryInfo.isRunning();
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            System.out.println("Impossible to get LOADER status!");
            return false;
        }
        return notRunning;
    }

}
