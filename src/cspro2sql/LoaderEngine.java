package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.Questionnaire;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.reader.QuestionnaireReader;
import cspro2sql.writer.InsertWriter;
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
import java.util.Date;
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
    private static final int MAX_COMMIT_SIZE = 100;
    private static final String ERROR_STMT = "insert into CSPRO2SQL_ERRORS (DICTIONARY, ERROR, DATE, CSPRO_GUID, QUESTIONNAIRE, SQL_SCRIPT) values (?,?,?,?,?,?)";
    private static final String LAST_REVISION_UPDATE_STMT = "update CSPRO2SQL_DICTIONARY set REVISION = ? where ID = ?";
    private static final String LAST_REVISION_SELECT_STMT = "select ID, REVISION from CSPRO2SQL_DICTIONARY where NAME = ?";
    private static final String LAST_REVISION_INSERT_STMT = "insert into CSPRO2SQL_DICTIONARY (NAME) values (?)";

    public static void main(String[] args) {
        Properties prop = new Properties();
        try (InputStream in = LoaderEngine.class.getResourceAsStream("/database.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            return;
        }
        try {
            Dictionary dictionary = DictionaryReader.read(
                    prop.getProperty("dictionary.filename"),
                    prop.getProperty("db.dest.table.prefix"));
            execute(dictionary, prop, true, true, false);
        } catch (Exception ex) {
            System.exit(1);
        }
    }

    static boolean execute(Dictionary dictionary, Properties prop, boolean allRecords, boolean checkConstraints, boolean checkOnly) {
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
            PreparedStatement selectLastRevisionStmt = connDst.prepareStatement(LAST_REVISION_SELECT_STMT);
            PreparedStatement dictionaryStmt = connDst.prepareStatement(LAST_REVISION_INSERT_STMT);
            PreparedStatement lastRevisionUpdateStmt = connDst.prepareStatement(LAST_REVISION_UPDATE_STMT);
            PreparedStatement getGuidStmt = connSrc.prepareStatement("select guid from " + srcSchema + "." + srcDataTable + " order by guid asc limit 1");
            PreparedStatement selectQuestionnaire;

            int idDictionary;
            int lastRevision;
            selectLastRevisionStmt.setString(1, srcDataTable);
            ResultSet result = selectLastRevisionStmt.executeQuery();
            if (result.next()) {
                idDictionary = result.getInt(1);
                lastRevision = result.getInt(2);
            } else {
                dictionaryStmt.setString(1, srcDataTable);
                dictionaryStmt.executeUpdate();
                connDst.commit();
                result = selectLastRevisionStmt.executeQuery();
                result.next();
                idDictionary = result.getInt(1);
                lastRevision = result.getInt(2);
            }
            lastRevisionUpdateStmt.setInt(2, idDictionary);
            // TODO retrieve max revision from source db
            int maxRevision = 100;

            //Get questionnaires from source database (CSPro plain text files)
            // TODO read last loaded guid
            boolean errors = false;
            result = getGuidStmt.executeQuery();
            if (!result.next()) {
                System.out.println("No data in the CsPro DB found!");
                return true;
            }
            InputStream firstGuid = result.getBinaryStream(1);
            firstGuid = null;
            
            if (allRecords) {
                if (firstGuid == null) {
                    selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " order by revision, guid limit " + MAX_COMMIT_SIZE);
                } else {
                    selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? order by revision, guid limit " + MAX_COMMIT_SIZE);
                    selectQuestionnaire.setBinaryStream(1, firstGuid);
                }
                result = selectQuestionnaire.executeQuery();
                selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? order by revision, guid limit " + MAX_COMMIT_SIZE);
                System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Starting data transfer from CsPro to MySql... [all records]");
            } else {
                if (firstGuid == null) {
                    selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where revision > ? AND revision <= ? order by revision, guid limit " + MAX_COMMIT_SIZE);
                    selectQuestionnaire.setInt(1, lastRevision);
                    selectQuestionnaire.setInt(2, maxRevision);
                } else {
                    selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? AND revision > ? AND revision <= ? order by revision, guid limit " + MAX_COMMIT_SIZE);
                    selectQuestionnaire.setBinaryStream(1, firstGuid);
                    selectQuestionnaire.setInt(2, lastRevision);
                    selectQuestionnaire.setInt(3, maxRevision);
                }
                result = selectQuestionnaire.executeQuery();
                selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? AND revision > ? AND revision <= ? order by revision, guid limit " + MAX_COMMIT_SIZE);
                selectQuestionnaire.setInt(2, lastRevision);
                selectQuestionnaire.setInt(3, maxRevision);
                System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Starting data transfer from CsPro to MySql... [" + lastRevision + " -> " + maxRevision + "]");
            }
            
            int totalCompleted = 0, total = 0;
            List<Questionnaire> quests = new LinkedList<>();
            while (result.next()) {
                String questionnaire = result.getString(1);
                InputStream guid = result.getBinaryStream(2);
                //Get the microdata parsing CSPro plain text files according to its dictionary
                Questionnaire microdata = QuestionnaireReader.parse(dictionary, questionnaire);
                microdata.setGuid(guid);
                microdata.setSchema(prop.getProperty("db.dest.schema"));
                total++;

                if ((checkConstraints || checkOnly) && !microdata.checkValueSets()) {
                    errors = true;
                    String msg = "Validation failed\n" + microdata.getCheckErrors();
                    writeError(idDictionary, msg, microdata, "", errorStmt);
                } else if (!checkOnly) {
                    quests.add(microdata);
                }

                if (result.isLast()) {
                    if (checkOnly) {
                        System.out.print("x");
                    } else {
                        int completed = commitList(dictionary, quests, idDictionary, stmtDst, errorStmt);
                        totalCompleted += completed;
                        if (completed == quests.size()) {
                            System.out.print("+");
                        } else {
                            System.out.print("-");
                            errors = true;
                        }
                        quests.clear();
                    }
                    selectQuestionnaire.setBinaryStream(1, guid);
                    result = selectQuestionnaire.executeQuery();
                }
            }
            System.out.println();

            if (!checkOnly) {
                lastRevisionUpdateStmt.setInt(1, maxRevision);
                lastRevisionUpdateStmt.executeUpdate();
                connDst.commit();
            }

            System.out.println("Loaded " + totalCompleted + " of " + total + " questionnaires");
            if (errors) {
                globalError = true;
                System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Data transfer completed with ERRORS (check error table)!");
            } else {
                System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Data transfer completed!");
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            try {
                if (connDst != null) {
                    connDst.rollback();
                }
            } catch (SQLException ex1) {
                LOGGER.log(Level.SEVERE, "Rollback failure", ex1);
            }
            LOGGER.log(Level.SEVERE, "Database exception", ex);
        } finally {
            try {
                if (connSrc != null) {
                    connSrc.close();
                }
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "Impossible to close the db conenction", ex);
            }
            try {
                if (connDst != null) {
                    connDst.close();
                }
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "Impossible to close the db conenction", ex);
            }
        }
        return !globalError;
    }

    private static int commitList(Dictionary dictionary, List<Questionnaire> quests, int idDictionary,
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
                    String msg = "Impossible to load questionnaire - " + e2.getMessage();
                    writeError(idDictionary, msg, q, script.toString(), errorStmt);
                    done--;
                }
            }
        }
        return done;
    }

    private static void writeError(int idDictionary, String msg, Questionnaire q, String script, PreparedStatement errorStmt) throws SQLException {
        Connection connDst = errorStmt.getConnection();
        if (msg.length() > 2048) {
            msg = msg.substring(0, 2048);
        }
        errorStmt.setInt(1, idDictionary);
        errorStmt.setString(2, msg);
        errorStmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        errorStmt.setBinaryStream(4, q.getGuid());
        errorStmt.setString(5, q.getPlainText());
        errorStmt.setString(6, script);
        errorStmt.executeUpdate();
        connDst.commit();
    }

}
