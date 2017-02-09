package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.DictionaryInfo;
import cspro2sql.bean.Questionnaire;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.reader.QuestionnaireReader;
import cspro2sql.sql.DictionaryQuery;
import cspro2sql.writer.InsertWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
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
    private static final int MAX_COMMIT_SIZE = 100;

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
            execute(dictionary, prop, true, false, false, false, false);
        } catch (Exception ex) {
            System.exit(1);
        }
    }

    static boolean execute(Dictionary dictionary, Properties prop, boolean allRecords, boolean checkConstraints, boolean checkOnly, boolean force, boolean recovery) {
        boolean errors = false;
        SimpleDateFormat SDF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss", Locale.getDefault());

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String srcSchema = prop.getProperty("db.source.schema");
            String srcDataTable = prop.getProperty("db.source.data.table");

            //Connect to the source database
            try (Connection connSrc = DriverManager.getConnection(
                    prop.getProperty("db.source.uri") + "/" + srcSchema + "?autoReconnect=true&useSSL=false",
                    prop.getProperty("db.source.username"),
                    prop.getProperty("db.source.password"))) {
                connSrc.setReadOnly(true);

                //Connect to the destination database
                try (Connection connDst = DriverManager.getConnection(
                        prop.getProperty("db.dest.uri") + "/" + prop.getProperty("db.dest.schema") + "?autoReconnect=true&useSSL=false",
                        prop.getProperty("db.dest.username"),
                        prop.getProperty("db.dest.password"))) {
                    connDst.setAutoCommit(false);

                    DictionaryQuery dictionaryQuery = new DictionaryQuery(connDst);

                    DictionaryInfo dictionaryInfo = dictionaryQuery.getDictionaryInfo(srcDataTable);
                    int idDictionary = dictionaryInfo.getId();
                    int lastRevision = dictionaryInfo.getRevision();

                    // TODO retrieve max revision from source db
                    int maxRevision = 100;

                    if ((dictionaryInfo = dictionaryQuery.run(idDictionary, force, recovery)) == null) {
                        System.out.println("An instance of the LOADER is still runnning!");
                        return false;
                    }

                    // TODO guid is not working
                    InputStream firstGuid = null;
                    if (recovery && !force) {
                        firstGuid = dictionaryInfo.getLastGuid();
                    }

                    ResultSet result;
                    PreparedStatement selectQuestionnaire;
                    if (allRecords) {
                        selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " order by guid limit " + MAX_COMMIT_SIZE);
                        result = selectQuestionnaire.executeQuery();
                        selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? order by guid limit " + MAX_COMMIT_SIZE);
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Starting data transfer from CsPro to MySql... [all records]");
                    } else {
                        if (firstGuid == null) {
                            selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where revision > ? AND revision <= ? order by guid limit " + MAX_COMMIT_SIZE);
                            selectQuestionnaire.setInt(1, lastRevision);
                            selectQuestionnaire.setInt(2, maxRevision);
                        } else {
                            selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? AND revision > ? AND revision <= ? order by guid limit " + MAX_COMMIT_SIZE);
                            selectQuestionnaire.setBinaryStream(1, firstGuid);
                            selectQuestionnaire.setInt(2, lastRevision);
                            selectQuestionnaire.setInt(3, maxRevision);
                        }
                        result = selectQuestionnaire.executeQuery();
                        selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid from " + srcSchema + "." + srcDataTable + " where guid > ? AND revision > ? AND revision <= ? order by guid limit " + MAX_COMMIT_SIZE);
                        selectQuestionnaire.setInt(2, lastRevision);
                        selectQuestionnaire.setInt(3, maxRevision);
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Starting data transfer from CsPro to MySql... [" + lastRevision + " -> " + maxRevision + "]");
                    }

                    int totalCompleted = dictionaryInfo.getLoaded();
                    int total = dictionaryInfo.getTotal();
                    try (Statement stmtDst = connDst.createStatement()) {
                        stmtDst.executeQuery("SET unique_checks=0");
                        stmtDst.executeQuery("SET foreign_key_checks=0");

                        boolean chunkError = false;
                        List<Questionnaire> quests = new LinkedList<>();
                        while (result.next()) {
                            String questionnaire = result.getString(1);
                            byte[] guid = result.getBytes(2);
                            //Get the microdata parsing CSPro plain text files according to its dictionary
                            Questionnaire microdata = QuestionnaireReader.parse(dictionary, questionnaire);
                            microdata.setGuid(guid);
                            microdata.setSchema(prop.getProperty("db.dest.schema"));
                            total++;

                            if ((checkConstraints || checkOnly) && !microdata.checkValueSets()) {
                                errors = true;
                                chunkError = true;
                                String msg = "Validation failed\n" + microdata.getCheckErrors();
                                dictionaryQuery.writeError(idDictionary, msg, microdata, "");
                            } else if (!checkOnly) {
                                quests.add(microdata);
                            }

                            if (result.isLast()) {
                                if (checkOnly) {
                                    System.out.print((chunkError) ? '-' : 'x');
                                } else {
                                    int completed = commitList(dictionary, quests, idDictionary, stmtDst, dictionaryQuery);
                                    totalCompleted += completed;
                                    dictionaryQuery.updateLoaded(idDictionary, totalCompleted, total, guid);
                                    if (completed != quests.size()) {
                                        chunkError = true;
                                    }
                                    if (chunkError) {
                                        System.out.print('-');
                                        errors = true;
                                    } else {
                                        System.out.print('+');
                                    }
                                }
                                quests.clear();
                                result.close();
                                chunkError = false;
                                selectQuestionnaire.setBytes(1, guid);
                                result = selectQuestionnaire.executeQuery();
                            }
                        }
                        System.out.println();

                        if (!checkOnly) {
                            dictionaryQuery.updateRevision(idDictionary, maxRevision);
                        }

                        stmtDst.executeQuery("SET foreign_key_checks=1");
                        stmtDst.executeQuery("SET unique_checks=1");
                    }

                    if (errors) {
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Data transfer completed with ERRORS (check error table)!");
                    } else {
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Data transfer completed!");
                    }
                    System.out.println("Loaded " + totalCompleted + " of " + total + " questionnaires");

                    if (!dictionaryQuery.stop(idDictionary)) {
                        System.out.println("Impossible to set LOADER status to stop!");
                        errors = false;
                    }
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            LOGGER.log(Level.SEVERE, "Database exception", ex);
        }
        return !errors;
    }

    private static int commitList(Dictionary dictionary, List<Questionnaire> quests, int idDictionary,
            Statement stmtDst, DictionaryQuery dictionaryQuery) throws SQLException {
        int done = quests.size();
        try {
            for (Questionnaire q : quests) {
                InsertWriter.create(q.getSchema(), dictionary, q, stmtDst);
            }
            stmtDst.getConnection().commit();
        } catch (Exception e1) {
            stmtDst.getConnection().rollback();
            for (Questionnaire q : quests) {
                StringBuilder script = new StringBuilder();
                try {
                    InsertWriter.create(q.getSchema(), dictionary, q, stmtDst, script);
                    stmtDst.getConnection().commit();
                } catch (Exception e2) {
                    stmtDst.getConnection().rollback();
                    String msg = "Impossible to load questionnaire - " + e2.getMessage();
                    dictionaryQuery.writeError(idDictionary, msg, q, script.toString());
                    done--;
                }
            }
        }
        return done;
    }

}
