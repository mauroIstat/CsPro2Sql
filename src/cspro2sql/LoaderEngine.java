
package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Questionnaire;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.reader.QuestionnaireReader;
import cspro2sql.writer.InsertWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Use this class to transfer date from a CsPro db to a MySql schema
 *
 * @author Istat Cooperation Unit
 */
public class LoaderEngine {

    private static final Logger LOGGER = Logger.getLogger(LoaderEngine.class.getName());
    private static final SimpleDateFormat SDF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static final int COMMIT_SIZE = 100;
    private static final String ERROR_STMT = "insert into CSPRO2SQL_ERRORS (ERROR, DATE, CSPRO_GUID, QUESTIONNAIRE, SQL_SCRIPT) values (?,?,?,?,?)";
    private static final String LAST_UPDATE_UPDATE_STMT = "update CSPRO2SQL_LASTUPDATE set LAST_UPDATE = ?";
    private static final String LAST_UPDATE_SELECT_STMT = "select LAST_UPDATE from CSPRO2SQL_LASTUPDATE";
    private static final String LAST_UPDATE_INSERT_STMT = "insert into CSPRO2SQL_LASTUPDATE values (?)";

    public static void main(String[] args) {
        execute("/database.properties", true);
    }

    static boolean execute(String propertiesFile, boolean allRecords) {
        Dictionary dictionary;
        Properties prop = new Properties();
        boolean isLocalFile = new File(propertiesFile).exists();
        //Load property file
        try (InputStream in
                = (isLocalFile
                        ? new FileInputStream(propertiesFile)
                        : LoaderEngine.class.getResourceAsStream(propertiesFile))) {
            prop.load(in);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Cannot read properties file", ex);
            return false;
        }

        //Parse dictionary file
        try {
            dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Impossible to read dictionary file", ex);
            return false;
        }

        Connection connSrc = null;
        Connection connDst = null;
        boolean globalError = false;

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String srcSchema = prop.getProperty("db.source.schema");
            String srcDataTable = prop.getProperty("db.source.data.table");

            //Connect to the source database
            connSrc = DriverManager.getConnection(
                    prop.getProperty("db.source.uri") + "/" + srcSchema + "?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.source.username"),
                    prop.getProperty("db.source.password"));
            connSrc.setReadOnly(true);

            //Connect to the destination database
            connDst = DriverManager.getConnection(
                    prop.getProperty("db.dest.uri") + "/" + prop.getProperty("db.dest.schema") + "?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.dest.username"),
                    prop.getProperty("db.dest.password"));
            connDst.setAutoCommit(false);

            Statement stmtDst = connDst.createStatement();
            PreparedStatement errorStmt = connDst.prepareStatement(ERROR_STMT);
            PreparedStatement lastUpdateStmt = connDst.prepareStatement(LAST_UPDATE_INSERT_STMT);
            PreparedStatement selectQuestionnaire;

            Timestamp now = new Timestamp(System.currentTimeMillis());
            Timestamp dTimestamp = new Timestamp(0);
            ResultSet result = stmtDst.executeQuery(LAST_UPDATE_SELECT_STMT);
            if (result.next()) {
                dTimestamp = result.getTimestamp(1);
                lastUpdateStmt = connDst.prepareStatement(LAST_UPDATE_UPDATE_STMT);
            }

            //Get questionnaires from source database (CSPro plain text files)
            boolean errors = false;
            if (allRecords) {
                selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " order by guid");
                System.out.println("Starting data transfer from CsPro to MySql... [all records]");
            } else {
                selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where modified_time >= ?  AND modified_time < ?  order by guid");
                selectQuestionnaire.setTimestamp(1, dTimestamp);
                selectQuestionnaire.setTimestamp(2, now);
                System.out.println("Starting data transfer from CsPro to MySql... [" + SDF.format(dTimestamp) + " -> " + SDF.format(now) + "]");
            }
            int totalCompleted = 0, total = 0;
            List<Questionnaire> quests = new LinkedList<>();
            result = selectQuestionnaire.executeQuery();
            while (result.next()) {
                String questionnaire = result.getString(1);
                InputStream binaryStream = result.getBinaryStream(2);
                //Get the microdata parsing CSPro plain text files according to its dictionary
                Questionnaire microdata = QuestionnaireReader.parse(dictionary, questionnaire);
                microdata.setGuid(binaryStream);
                microdata.setSchema(prop.getProperty("db.dest.schema"));
                quests.add(microdata);
                total++;
                if (quests.size() == COMMIT_SIZE) {
                    int completed = commitList(dictionary, quests, stmtDst, errorStmt);
                    totalCompleted += completed;
                    if (completed==quests.size()) {
                        System.out.print("+");
                    } else {
                        System.out.print("-");
                        errors = true;
                    }
                    quests.clear();
                }
            }
            if (!quests.isEmpty()) {
                int completed = commitList(dictionary, quests, stmtDst, errorStmt);
                totalCompleted += completed;
                if (completed==quests.size()) {
                    System.out.print("+");
                } else {
                    System.out.print("-");
                    errors = true;
                }
                quests.clear();
            }
            System.out.println();

            lastUpdateStmt.setTimestamp(1, now);
            lastUpdateStmt.executeUpdate();
            connDst.commit();

            System.out.println("Loaded "+totalCompleted+" of "+total+" questionnaires");
            if (errors) {
                globalError = true;
                System.out.println("Data transfer completed with ERRORS (check error table)!");
            } else {
                System.out.println("Data transfer completed!");
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            try {
                if (connDst!=null) connDst.rollback();
            } catch (SQLException ex1) {
                LOGGER.log(Level.SEVERE, "Rollback failure", ex1);
            }
            LOGGER.log(Level.SEVERE, "Database exception", ex);
        } finally {
            try {
                if (connSrc!=null) connSrc.close();
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "Impossible to close the db conenction", ex);
            }
            try {
                if (connDst!=null) connDst.close();
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "Impossible to close the db conenction", ex);
            }
        }
        return !globalError;
    }

    private static int commitList(Dictionary dictionary, List<Questionnaire> quests,
            Statement stmtDst, PreparedStatement errorStmt) throws SQLException {
        Connection connDst = stmtDst.getConnection();
        int done = quests.size();
        try {
            for (Questionnaire q : quests) {
                InsertWriter.create(q.getSchema(), dictionary, q, stmtDst);
            }
            connDst.commit();
        } catch (Exception e1) {
            connDst.rollback();
            for (Questionnaire q : quests) {
                StringBuilder script = new StringBuilder();
                try {
                    InsertWriter.create(q.getSchema(), dictionary, q, stmtDst, script);
                    connDst.commit();
                } catch (Exception e2) {
                    connDst.rollback();
                    String msg = "Impossible to transfer questionnaire - " + e2.getMessage();
                    if (msg.length() > 2048) {
                        msg = msg.substring(0, 2048);
                    }
                    errorStmt.setString(1, msg);
                    errorStmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                    errorStmt.setBinaryStream(3, q.getGuid());
                    errorStmt.setString(4, q.getPlainText());
                    errorStmt.setString(5, script.toString());
                    errorStmt.executeUpdate();
                    connDst.commit();
                    done--;
                }
            }
        }
        return done;
    }

}
