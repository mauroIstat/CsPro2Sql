package cspro2sql;

import cspro2sql.bean.Dictionary;
import cspro2sql.bean.DictionaryInfo;
import cspro2sql.bean.Questionnaire;
import cspro2sql.reader.DictionaryReader;
import cspro2sql.reader.QuestionnaireReader;
import cspro2sql.sql.DictionaryQuery;
import cspro2sql.writer.DeleteWriter;
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
 * Copyright 2017 ISTAT
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence. You may
 * obtain a copy of the Licence at:
 *
 * http://ec.europa.eu/idabc/eupl5
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * Licence for the specific language governing permissions and limitations under
 * the Licence.
 *
 * @author Guido Drovandi <drovandi @ istat.it>
 * @author Mauro Bruno <mbruno @ istat.it>
 * @version 0.9
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

                    int nextRevision;
                    byte[] firstGuid;
                    if (recovery && !force) {
                        firstGuid = dictionaryInfo.getLastGuid();
                        nextRevision = dictionaryInfo.getNextRevision();
                    } else {
                        firstGuid = null;
                        try (Statement stmt = connSrc.createStatement()) {
                            try (ResultSet r = stmt.executeQuery("select max(revision) from " + srcSchema + "." + srcDataTable)) {
                                r.next();
                                nextRevision = r.getInt(1);
                            }
                        }
                    }

                    dictionaryInfo.setNextRevision(nextRevision);
                    if ((dictionaryInfo = dictionaryQuery.run(dictionaryInfo, force, recovery)) == null) {
                        System.out.println("An instance of the LOADER is still runnning!");
                        return false;
                    }

                    ResultSet result;
                    PreparedStatement selectQuestionnaire;
                    if (allRecords) {
                        selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid, deleted from " + srcSchema + "." + srcDataTable + " order by guid limit " + MAX_COMMIT_SIZE);
                        result = selectQuestionnaire.executeQuery();
                        selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid, deleted from " + srcSchema + "." + srcDataTable + " where guid > ? order by guid limit " + MAX_COMMIT_SIZE);
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Starting data transfer from CsPro to MySql... [all records]");
                    } else {
                        if (firstGuid == null) {
                            selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid, deleted from " + srcSchema + "." + srcDataTable + " where revision > ? AND revision <= ? order by guid limit " + MAX_COMMIT_SIZE);
                            selectQuestionnaire.setInt(1, lastRevision);
                            selectQuestionnaire.setInt(2, nextRevision);
                        } else {
                            selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid, deleted from " + srcSchema + "." + srcDataTable + " where guid > ? AND revision > ? AND revision <= ? order by guid limit " + MAX_COMMIT_SIZE);
                            selectQuestionnaire.setBytes(1, firstGuid);
                            selectQuestionnaire.setInt(2, lastRevision);
                            selectQuestionnaire.setInt(3, nextRevision);
                        }
                        result = selectQuestionnaire.executeQuery();
                        selectQuestionnaire = connSrc.prepareStatement("select questionnaire, guid, deleted from " + srcSchema + "." + srcDataTable + " where guid > ? AND revision > ? AND revision <= ? order by guid limit " + MAX_COMMIT_SIZE);
                        selectQuestionnaire.setInt(2, lastRevision);
                        selectQuestionnaire.setInt(3, nextRevision);
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Starting data transfer from CsPro to MySql... [" + lastRevision + " -> " + nextRevision + "]");
                    }

                    try (Statement stmtDst = connDst.createStatement()) {
                        stmtDst.executeQuery("SET unique_checks=0");
                        stmtDst.executeQuery("SET foreign_key_checks=0");

                        boolean chunkError = false;
                        List<Questionnaire> quests = new LinkedList<>();
                        while (result.next()) {
                            String questionnaire = result.getString(1);
                            byte[] guid = result.getBytes(2);
                            boolean deleted = result.getInt(3) == 1;

                            //Get the microdata parsing CSPro plain text files according to its dictionary
                            Questionnaire microdata = QuestionnaireReader.parse(dictionary, questionnaire, prop.getProperty("db.dest.schema"), guid, deleted);
                            dictionaryInfo.incTotal();
                            dictionaryInfo.setLastGuid(guid);

                            if ((checkConstraints || checkOnly) && !microdata.isDeleted() && !microdata.checkValueSets()) {
                                errors = true;
                                chunkError = true;
                                String msg = "Validation failed\n" + microdata.getCheckErrors();
                                dictionaryQuery.writeError(dictionaryInfo, msg, microdata, "");
                                dictionaryInfo.incErrors();
                            } else if (!checkOnly) {
                                quests.add(microdata);
                            }

                            if (result.isLast()) {
                                if (checkOnly) {
                                    System.out.print((chunkError) ? '-' : 'x');
                                } else {
                                    chunkError |= commitList(dictionary, quests, idDictionary, stmtDst, dictionaryQuery, dictionaryInfo);
                                    dictionaryQuery.updateLoaded(dictionaryInfo);
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
                            dictionaryQuery.updateRevision(dictionaryInfo);
                        }

                        stmtDst.executeQuery("SET foreign_key_checks=1");
                        stmtDst.executeQuery("SET unique_checks=1");
                    }

                    if (errors) {
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Data transfer completed with ERRORS (check error table)!");
                    } else {
                        System.out.println(SDF.format(new Date(System.currentTimeMillis())) + " Data transfer completed!");
                    }
                    dictionaryInfo.printShort(System.out);

                    if (!dictionaryQuery.stop(dictionaryInfo)) {
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

    private static boolean commitList(Dictionary dictionary, List<Questionnaire> quests, int idDictionary,
            Statement stmtDst, DictionaryQuery dictionaryQuery, DictionaryInfo dictionaryInfo) throws SQLException {
        boolean error = false;
        int deleted = 0;
        int loaded = 0;
        try {
            for (Questionnaire q : quests) {
                if (q.isDeleted()) {
                    DeleteWriter.create(q.getSchema(), dictionary, q, stmtDst);
                    deleted++;
                } else {
                    InsertWriter.create(q.getSchema(), dictionary, q, stmtDst);
                    loaded++;
                }
            }
            stmtDst.getConnection().commit();
        } catch (Exception e1) {
            stmtDst.getConnection().rollback();
            deleted = 0;
            loaded = 0;
            for (Questionnaire q : quests) {
                StringBuilder script = new StringBuilder();
                try {
                    if (q.isDeleted()) {
                        DeleteWriter.create(q.getSchema(), dictionary, q, stmtDst);
                    } else {
                        InsertWriter.create(q.getSchema(), dictionary, q, stmtDst, script);
                    }
                    stmtDst.getConnection().commit();
                    if (q.isDeleted()) {
                        deleted++;
                    } else {
                        loaded++;
                    }
                } catch (Exception e2) {
                    stmtDst.getConnection().rollback();
                    error = true;
                    String msg = "Impossible to load questionnaire - " + e2.getMessage();
                    dictionaryQuery.writeError(dictionaryInfo, msg, q, script.toString());
                    dictionaryInfo.incErrors();
                }
            }
        }
        dictionaryInfo.incLoaded(loaded);
        dictionaryInfo.incDeleted(deleted);
        return error;
    }

}
